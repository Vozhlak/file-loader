package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

var ErrRangeIgnored = errors.New("server ignored Range header")

type FileMeta struct {
	FileName     string
	FileSize     int64
	AcceptRanges bool
}

type Chunk struct {
	Index int
	Start int64
	End   int64
	Size  int64
}

type DownloadState struct {
	URL              string `json:"url"`
	TotalSize        int64  `json:"total_size"`
	ChunkSize        int    `json:"chunk_size"`
	TotalChunks      int    `json:"total_chunks"`
	DownloadedChunks []bool `json:"downloaded_chunks"`
}

const chunkSize = 10 * 1024 * 1024
const maxRetries = 3
const retryDelay = 2 * time.Second
const maxConcurrentChunks = 8
const shutdownTimeout = 5 * time.Second
const maxPasses = 10

func detectFileName(resp *http.Response) string {
	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition != "" {
		_, params, err := mime.ParseMediaType(contentDisposition)
		if err == nil {
			if filename := params["filename"]; filename != "" {
				return filepath.Base(filename)
			}
			if filename := params["filename*"]; filename != "" {
				return filepath.Base(filename)
			}
		}
	}

	name := path.Base(resp.Request.URL.Path)
	if name == "." || name == "/" || name == "" {
		return ""
	}

	return filepath.Base(name)
}

func FetchMetaData(ctx context.Context, client *http.Client, rawURL string) (FileMeta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return FileMeta{}, fmt.Errorf("не удалось создать HEAD-запрос: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return FileMeta{}, fmt.Errorf("ошибка HEAD-запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return FileMeta{}, fmt.Errorf("ошибка: сервер вернул %d", resp.StatusCode)
	}

	name := detectFileName(resp)
	if name == "." || name == "/" || name == "" {
		return FileMeta{}, fmt.Errorf("не удалось определить имя файла из URL")
	}

	contentLength := resp.Header.Get("Content-Length")
	size, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil || size <= 0 {
		return FileMeta{}, fmt.Errorf("не удалось определить размер файла для %s", name)
	}

	supportResume := resp.Header.Get("Accept-Ranges") == "bytes"

	return FileMeta{
		FileName:     name,
		FileSize:     size,
		AcceptRanges: supportResume,
	}, nil
}

func createSparseFile(filePath string, size int64) (*os.File, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("не удалось создать директорию %s: %w", dir, err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	if err = file.Truncate(size); err != nil {
		file.Close()
		return nil, err
	}

	return file, nil
}

func buildChunks(fileSize, chunkSize int64) []Chunk {
	if chunkSize <= 0 || fileSize <= 0 {
		return nil
	}

	totalChunks := (fileSize + chunkSize - 1) / chunkSize
	chunks := make([]Chunk, 0, totalChunks)

	for i := int64(0); i < totalChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize - 1
		if end >= fileSize {
			end = fileSize - 1
		}

		chunks = append(chunks, Chunk{
			Index: int(i),
			Start: start,
			End:   end,
			Size:  end - start + 1,
		})
	}

	return chunks
}

func saveProgressAtomic(progressPath string, state DownloadState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := progressPath + ".tmp"
	if err = os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, progressPath)
}

func saveProgressWithLock(progressPath string, state *DownloadState, stateMu *sync.Mutex) error {
	stateMu.Lock()
	defer stateMu.Unlock()
	return saveProgressAtomic(progressPath, *state)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRangeIgnored) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() || netErr.Temporary() {
			return true
		}
	}

	var statusCode int
	if _, scanErr := fmt.Sscanf(err.Error(), "ожидали 206 Partial Content, получили %d", &statusCode); scanErr == nil {
		if statusCode >= 400 && statusCode < 500 {
			return false
		}
		if statusCode >= 500 {
			return true
		}
	}

	return true
}

func isContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func sendErr(errCh chan<- error, err error) {
	if err == nil {
		return
	}
	select {
	case errCh <- err:
	default:
		fmt.Fprintf(os.Stderr, "Ошибка (drop from errCh): %v\n", err)
	}
}

func downloadChunkData(ctx context.Context, client *http.Client, downloadURL string, chunk Chunk) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать запрос для чанка %d: %w", chunk.Index+1, err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.Start, chunk.End))

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil, fmt.Errorf("%w: получили 200 OK вместо 206 Partial Content", ErrRangeIgnored)
	}

	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("ожидали 206 Partial Content, получили %d", resp.StatusCode)
	}

	if resp.Header.Get("Content-Range") == "" {
		return nil, fmt.Errorf("для чанка %d сервер не прислал Content-Range", chunk.Index+1)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать тело чанка %d: %w", chunk.Index+1, err)
	}

	if int64(len(data)) != chunk.Size {
		return nil, fmt.Errorf("неполный чанк %d: ожидали %d байт, получили %d", chunk.Index+1, chunk.Size, len(data))
	}

	return data, nil
}

func downloadChunkWithRetry(ctx context.Context, client *http.Client, downloadURL string, chunk Chunk) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := downloadChunkData(ctx, client, downloadURL, chunk)
		if err == nil {
			return data, nil
		}

		lastErr = err

		if !isRetryableError(err) {
			return nil, err
		}

		if attempt < maxRetries-1 {
			fmt.Fprintf(os.Stderr, "Чанк %d: ошибка: %v\n  Повторная попытка через %v (%d/%d)...\n", chunk.Index+1, err, retryDelay, attempt+1, maxRetries)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryDelay):
			}
		}
	}

	return nil, lastErr
}

func writeChunk(file *os.File, chunk Chunk, data []byte, fileMu *sync.Mutex) error {
	fileMu.Lock()
	defer fileMu.Unlock()

	written, err := file.WriteAt(data, chunk.Start)
	if err != nil {
		return fmt.Errorf("ошибка записи чанка %d: %w", chunk.Index+1, err)
	}

	if int64(written) != chunk.Size {
		return fmt.Errorf("неполная запись чанка %d: ожидали %d байт, записали %d", chunk.Index+1, chunk.Size, written)
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("не удалось синхронизировать файл после чанка %d: %w", chunk.Index+1, err)
	}

	return nil
}

func markChunkDownloaded(state *DownloadState, chunk Chunk, progressPath string, stateMu *sync.Mutex) error {
	stateMu.Lock()
	defer stateMu.Unlock()

	if chunk.Index < 0 || chunk.Index >= len(state.DownloadedChunks) {
		return fmt.Errorf("некорректный индекс чанка %d", chunk.Index)
	}
	if state.DownloadedChunks[chunk.Index] {
		return nil
	}

	state.DownloadedChunks[chunk.Index] = true
	if err := saveProgressAtomic(progressPath, *state); err != nil {
		state.DownloadedChunks[chunk.Index] = false
		return fmt.Errorf("не удалось сохранить прогресс для чанка %d: %w", chunk.Index+1, err)
	}

	return nil
}

func isAllChunksDownloaded(state *DownloadState) bool {
	for _, downloaded := range state.DownloadedChunks {
		if !downloaded {
			return false
		}
	}

	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func calcDownloadedBytes(chunks []Chunk, downloaded []bool) int64 {
	var total int64
	for i, done := range downloaded {
		if done {
			total += chunks[i].Size
		}
	}
	return total
}

func countRemainingChunks(downloaded []bool) int {
	remaining := 0
	for _, done := range downloaded {
		if !done {
			remaining++
		}
	}
	return remaining
}

func chunkWorker(
	ctx context.Context,
	workerID int,
	jobs <-chan Chunk,
	client *http.Client,
	downloadURL string,
	file *os.File,
	state *DownloadState,
	progressPath string,
	fileMu *sync.Mutex,
	stateMu *sync.Mutex,
	totalChunks int,
	wg *sync.WaitGroup,
	progressBar *mpb.Bar,
) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case chunk, ok := <-jobs:
			if !ok {
				return
			}

			data, err := downloadChunkWithRetry(ctx, client, downloadURL, chunk)
			if err != nil {
				if !isContextCancellation(err) {
					if !isRetryableError(err) {
						fmt.Fprintf(os.Stderr, "[Worker %d] Чанк %d/%d: постоянная ошибка: %v\n", workerID, chunk.Index+1, totalChunks, err)
					} else {
						fmt.Fprintf(os.Stderr, "[Worker %d] Чанк %d/%d: не удалось загрузить после %d попыток: %v\n", workerID, chunk.Index+1, totalChunks, maxRetries, err)
					}
				}
				continue
			}

			if err = writeChunk(file, chunk, data, fileMu); err != nil {
				fmt.Fprintf(os.Stderr, "[Worker %d] Чанк %d/%d: ошибка записи: %v\n", workerID, chunk.Index+1, totalChunks, err)
				continue
			}

			if err = markChunkDownloaded(state, chunk, progressPath, stateMu); err != nil {
				fmt.Fprintf(os.Stderr, "[Worker %d] Чанк %d/%d: ошибка сохранения прогресса: %v\n", workerID, chunk.Index+1, totalChunks, err)
				continue
			}

			progressBar.IncrBy(int(chunk.Size))
		}
	}
}

func loadOrInitState(fullPath, progressPath string, meta FileMeta, sourceURL string, chunks []Chunk) (*os.File, DownloadState, int64, error) {
	state := DownloadState{
		URL:              sourceURL,
		TotalSize:        meta.FileSize,
		ChunkSize:        chunkSize,
		TotalChunks:      len(chunks),
		DownloadedChunks: make([]bool, len(chunks)),
	}

	if _, err := os.Stat(progressPath); err == nil {
		data, err := os.ReadFile(progressPath)
		if err != nil {
			return nil, DownloadState{}, 0, fmt.Errorf("не удалось прочитать файл состояния %s: %w", progressPath, err)
		}

		if err = json.Unmarshal(data, &state); err != nil {
			return nil, DownloadState{}, 0, fmt.Errorf("не удалось восстановить состояние из %s: %w", progressPath, err)
		}

		if state.URL != sourceURL || state.TotalSize != meta.FileSize || state.TotalChunks != len(chunks) || len(state.DownloadedChunks) != len(chunks) {
			return nil, DownloadState{}, 0, fmt.Errorf("некорректный файл состояния %s", progressPath)
		}

		file, err := os.OpenFile(fullPath, os.O_RDWR, 0644)
		if err != nil {
			return nil, DownloadState{}, 0, fmt.Errorf("не удалось открыть файл для докачки %s: %w", fullPath, err)
		}

		fi, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, DownloadState{}, 0, fmt.Errorf("не удалось получить размер файла %s: %w", fullPath, err)
		}

		if fi.Size() != meta.FileSize {
			if err = file.Truncate(meta.FileSize); err != nil {
				file.Close()
				return nil, DownloadState{}, 0, fmt.Errorf("не удалось восстановить размер файла %s: %w", fullPath, err)
			}
		}

		downloadedBytes := calcDownloadedBytes(chunks, state.DownloadedChunks)
		fmt.Fprintf(os.Stderr, "Найден файл состояния, возобновляем загрузку: %s\n", progressPath)
		return file, state, downloadedBytes, nil
	} else if !os.IsNotExist(err) {
		return nil, DownloadState{}, 0, fmt.Errorf("не удалось проверить файл состояния %s: %w", progressPath, err)
	}

	file, err := createSparseFile(fullPath, meta.FileSize)
	if err != nil {
		return nil, DownloadState{}, 0, fmt.Errorf("ошибка подготовки файла %s: %w", meta.FileName, err)
	}

	if err = saveProgressAtomic(progressPath, state); err != nil {
		file.Close()
		return nil, DownloadState{}, 0, fmt.Errorf("не удалось записать файл состояния для %s: %w", meta.FileName, err)
	}

	fmt.Fprintf(os.Stderr, "Создан файл состояния: %s\n", progressPath)
	return file, state, 0, nil
}

func finalizeShutdown(progressPath string, state *DownloadState, stateMu *sync.Mutex) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- saveProgressWithLock(progressPath, state, stateMu)
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("graceful shutdown: не удалось сохранить прогресс %s: %w", progressPath, err)
		}
		return nil
	case <-shutdownCtx.Done():
		return fmt.Errorf("graceful shutdown: таймаут сохранения прогресса %s: %w", progressPath, shutdownCtx.Err())
	}
}

func runDownload(rootCtx context.Context, p *mpb.Progress, client *http.Client, savePath, sourceURL string) error {
	meta, err := FetchMetaData(rootCtx, client, sourceURL)
	if err != nil {
		return fmt.Errorf("ошибка для %s: %w", sourceURL, err)
	}

	if !meta.AcceptRanges {
		return fmt.Errorf("сервер для %s не поддерживает Range-запросы", sourceURL)
	}

	fullPath := filepath.Join(savePath, meta.FileName)
	progressPath := fullPath + ".progress"
	chunks := buildChunks(meta.FileSize, chunkSize)
	if len(chunks) == 0 {
		return fmt.Errorf("не удалось построить чанки для %s", meta.FileName)
	}

	file, state, downloadedBytes, err := loadOrInitState(fullPath, progressPath, meta, sourceURL, chunks)
	if err != nil {
		return err
	}
	defer file.Close()

	bar := p.AddBar(meta.FileSize,
		mpb.PrependDecorators(
			decor.Name(meta.FileName+" "),
		),
		mpb.AppendDecorators(
			decor.CountersKibiByte("%.1f / %.1f "),
			decor.Percentage(),
		),
	)
	bar.SetCurrent(downloadedBytes)
	defer bar.SetTotal(meta.FileSize, true)

	workerCount := min(maxConcurrentChunks, len(chunks))
	var stateMu sync.Mutex
	var fileMu sync.Mutex

passes:
	for pass := 1; pass <= maxPasses; pass++ {
		if rootCtx.Err() != nil {
			break
		}

		remaining := countRemainingChunks(state.DownloadedChunks)
		if remaining == 0 {
			break
		}

		fmt.Fprintf(os.Stderr, "Проход %d/%d: осталось чанков %d\n", pass, maxPasses, remaining)

		jobs := make(chan Chunk)
		var workerWG sync.WaitGroup

		for w := 1; w <= workerCount; w++ {
			workerWG.Add(1)
			go chunkWorker(
				rootCtx,
				w,
				jobs,
				client,
				sourceURL,
				file,
				&state,
				progressPath,
				&fileMu,
				&stateMu,
				len(chunks),
				&workerWG,
				bar,
			)
		}

		for _, chunk := range chunks {
			if state.DownloadedChunks[chunk.Index] {
				continue
			}

			select {
			case <-rootCtx.Done():
				close(jobs)
				workerWG.Wait()
				break passes
			case jobs <- chunk:
			}
		}

		close(jobs)
		workerWG.Wait()

		if isAllChunksDownloaded(&state) {
			break
		}

		select {
		case <-rootCtx.Done():
			break passes
		case <-time.After(retryDelay):
		}
	}

	if rootCtx.Err() != nil {
		if err = finalizeShutdown(progressPath, &state, &stateMu); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Завершение по сигналу, прогресс сохранён: %s\n", progressPath)
		return nil
	}

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("не удалось получить финальный размер файла %s: %w", fullPath, err)
	}

	if isAllChunksDownloaded(&state) && fi.Size() == meta.FileSize {
		bar.SetCurrent(meta.FileSize)
		if err = os.Remove(progressPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("не удалось удалить файл состояния %s: %w", progressPath, err)
		}
		fmt.Fprintf(os.Stderr, "Завершено: %s\n", meta.FileName)
		return nil
	}

	if err = saveProgressWithLock(progressPath, &state, &stateMu); err != nil {
		return fmt.Errorf("не удалось сохранить итоговый прогресс %s: %w", progressPath, err)
	}

	return fmt.Errorf(
		"загрузка %s завершена не полностью: downloaded=%d/%d, файл состояния сохранён в %s",
		meta.FileName,
		calcDownloadedBytes(chunks, state.DownloadedChunks),
		meta.FileSize,
		progressPath,
	)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstSignalCh := make(chan os.Signal, 1)
	signal.Notify(firstSignalCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(firstSignalCh)

	go func() {
		<-firstSignalCh

		fmt.Fprintln(os.Stderr, "\nПолучен сигнал остановки. Завершаем текущие операции... Нажмите Ctrl+C ещё раз для принудительного выхода.")
		cancel()

		forceExitCh := make(chan os.Signal, 1)
		signal.Notify(forceExitCh, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(forceExitCh)

		<-forceExitCh
		fmt.Fprintln(os.Stderr, "\nПолучен повторный сигнал. Принудительное завершение.")
		os.Exit(130)
	}()

	savePath := os.Args[1]
	urls := os.Args[2:]

	client := &http.Client{Timeout: 30 * time.Second}

	var wg sync.WaitGroup
	errCh := make(chan error, len(urls)*4)
	p := mpb.New(mpb.WithOutput(os.Stderr))

	for _, rawURL := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			if err := runDownload(rootCtx, p, client, savePath, u); err != nil {
				sendErr(errCh, err)
			}
		}(rawURL)
	}

	wg.Wait()
	p.Wait()
	close(errCh)

	hadErrors := false
	for err := range errCh {
		if err != nil {
			hadErrors = true
			fmt.Fprintf(os.Stderr, "Ошибка: %v\n", err)
		}
	}

	if rootCtx.Err() != nil {
		fmt.Fprintln(os.Stderr, "Приложение остановлено корректно по сигналу.")
		return
	}

	if hadErrors {
		fmt.Fprintln(os.Stderr, "Загрузка завершена с ошибками.")
	} else {
		fmt.Fprintln(os.Stderr, "Все файлы успешно загружены!")
	}
}
