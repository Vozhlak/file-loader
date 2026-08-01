package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
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

const chunkSize = 10 * 1024 * 1024 // 10 MB
const maxRetries = 3
const retryDelay = 2 * time.Second
const maxConcurrentChunks = 8

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

func FetchMetaData(client *http.Client, rawURL string) (FileMeta, error) {
	downloadURL := rawURL

	resp, err := client.Head(downloadURL)
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

func saveProgress(progressPath string, state DownloadState) error {
	data, err := json.MarshalIndent(state, "", " ")
	if err != nil {
		return err
	}

	return os.WriteFile(progressPath, data, 0644)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrRangeIgnored) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		// timeout почти всегда ретрабелен
		if netErr.Timeout() {
			return true
		}
		// Temporary устаревающий по смыслу, но для учебной задачи ок как сигнал временной ошибки
		if netErr.Temporary() {
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

func downloadChunkData(client *http.Client, downloadURL string, chunk Chunk) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, downloadURL, nil)
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

func downloadChunkWithRetry(client *http.Client, downloadURL string, chunk Chunk) ([]byte, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		data, err := downloadChunkData(client, downloadURL, chunk)
		if err == nil {
			return data, nil
		}

		lastErr = err

		if !isRetryableError(err) {
			return nil, err
		}

		if attempt < maxRetries-1 {
			fmt.Printf(
				"Чанк %d: ошибка: %v\n  Повторная попытка через %v (%d/%d)...\n",
				chunk.Index+1,
				err,
				retryDelay,
				attempt+1,
				maxRetries,
			)
			time.Sleep(retryDelay)
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

	state.DownloadedChunks[chunk.Index] = true

	if err := saveProgress(progressPath, *state); err != nil {
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

func chunkWorker(
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
	progressBar *mpb.Bar) {
	defer wg.Done()

	for chunk := range jobs {
		data, err := downloadChunkWithRetry(client, downloadURL, chunk)
		if err != nil {
			if !isRetryableError(err) {
				fmt.Printf("[Worker %d] Чанк %d/%d: постоянная ошибка: %v\n", workerID, chunk.Index+1, totalChunks, err)
			} else {
				fmt.Printf("[Worker %d] Чанк %d/%d: не удалось загрузить после %d попыток: %v\n", workerID, chunk.Index+1, totalChunks, maxRetries, err)
			}
			continue
		}

		if err = writeChunk(file, chunk, data, fileMu); err != nil {
			fmt.Printf("[Worker %d] Чанк %d/%d: ошибка записи: %v\n", workerID, chunk.Index+1, totalChunks, err)
			continue
		}

		if err = markChunkDownloaded(state, chunk, progressPath, stateMu); err != nil {
			fmt.Printf("[Worker %d] Чанк %d/%d: ошибка сохранения прогресса: %v\n", workerID, chunk.Index+1, totalChunks, err)
			continue
		}

		progressBar.IncrBy(int(chunk.Size))
	}
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

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePath := os.Args[1]
	urls := os.Args[2:]

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(urls))

	p := mpb.New()

	for _, rawURL := range urls {
		wg.Add(1)

		go func(u string) {
			defer wg.Done()

			meta, err := FetchMetaData(client, u)
			if err != nil {
				errCh <- fmt.Errorf("ошибка для %s: %w", u, err)
				return
			}

			fullPath := filepath.Join(savePath, meta.FileName)
			progressPath := fullPath + ".progress"
			chunks := buildChunks(meta.FileSize, chunkSize)

			if len(chunks) == 0 {
				errCh <- fmt.Errorf("не удалось построить чанки для %s", meta.FileName)
				return
			}

			state := DownloadState{
				URL:              u,
				TotalSize:        meta.FileSize,
				ChunkSize:        chunkSize,
				TotalChunks:      len(chunks),
				DownloadedChunks: make([]bool, len(chunks)),
			}

			var file *os.File
			var downloadedBytes int64

			if _, err = os.Stat(progressPath); err == nil {
				data, err := os.ReadFile(progressPath)
				if err != nil {
					errCh <- fmt.Errorf("не удалось прочитать файл состояния %s: %w", progressPath, err)
					return
				}

				if err = json.Unmarshal(data, &state); err != nil {
					errCh <- fmt.Errorf("не удалось восстановить состояние из %s: %w", progressPath, err)
					return
				}

				if state.TotalSize != meta.FileSize {
					errCh <- fmt.Errorf(
						"некорректный файл состояния %s: total_size=%d, ожидалось %d",
						progressPath,
						state.TotalSize,
						meta.FileSize,
					)
					return
				}

				if state.TotalChunks != len(chunks) {
					errCh <- fmt.Errorf(
						"некорректный файл состояния %s: total_chunks=%d, ожидалось %d",
						progressPath,
						state.TotalChunks,
						len(chunks),
					)
					return
				}

				if len(state.DownloadedChunks) != len(chunks) {
					errCh <- fmt.Errorf(
						"некорректный файл состояния %s: downloaded_chunks=%d, ожидалось %d",
						progressPath,
						len(state.DownloadedChunks),
						len(chunks),
					)
					return
				}

				file, err = os.OpenFile(fullPath, os.O_RDWR, 0644)
				if err != nil {
					errCh <- fmt.Errorf("не удалось открыть файл для докачки %s: %w", fullPath, err)
					return
				}

				fi, err := file.Stat()
				if err != nil {
					file.Close()
					errCh <- fmt.Errorf("не удалось получить размер файла %s: %w", fullPath, err)
					return
				}

				if fi.Size() != meta.FileSize {
					if err = file.Truncate(meta.FileSize); err != nil {
						file.Close()
						errCh <- fmt.Errorf("не удалось восстановить размер файла %s: %w", fullPath, err)
						return
					}
				}

				downloadedBytes = calcDownloadedBytes(chunks, state.DownloadedChunks)
				fmt.Printf("Найден файл состояния, возобновляем загрузку: %s\n", progressPath)
			} else if os.IsNotExist(err) {
				file, err = createSparseFile(fullPath, meta.FileSize)
				if err != nil {
					errCh <- fmt.Errorf("ошибка подготовки файла %s: %w", meta.FileName, err)
					return
				}

				if err = saveProgress(progressPath, state); err != nil {
					file.Close()
					errCh <- fmt.Errorf("не удалось записать файл состояния для %s: %w", meta.FileName, err)
					return
				}

				downloadedBytes = 0
				fmt.Printf("Создан файл состояния: %s\n", progressPath)
			} else {
				errCh <- fmt.Errorf("не удалось проверить файл состояния %s: %w", progressPath, err)
				return
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

			workerCount := min(maxConcurrentChunks, len(chunks))
			var fileMu sync.Mutex
			var stateMu sync.Mutex

			const maxPasses = 10

			for pass := 1; pass <= maxPasses; pass++ {
				remaining := 0
				for _, done := range state.DownloadedChunks {
					if !done {
						remaining++
					}
				}

				if remaining == 0 {
					break
				}

				fmt.Printf("Проход %d/%d: осталось чанков %d\n", pass, maxPasses, remaining)

				jobs := make(chan Chunk)
				var workerWG sync.WaitGroup

				for w := 1; w <= workerCount; w++ {
					workerWG.Add(1)
					go chunkWorker(
						w,
						jobs,
						client,
						u,
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
					jobs <- chunk
				}

				close(jobs)
				workerWG.Wait()

				if isAllChunksDownloaded(&state) {
					break
				}

				time.Sleep(retryDelay)
			}

			fi, statErr := file.Stat()
			if statErr != nil {
				errCh <- fmt.Errorf("не удалось получить финальный размер файла %s: %w", fullPath, statErr)
				return
			}

			if isAllChunksDownloaded(&state) && fi.Size() == meta.FileSize {
				bar.SetCurrent(meta.FileSize)

				if err = os.Remove(progressPath); err != nil && !os.IsNotExist(err) {
					errCh <- fmt.Errorf("не удалось удалить файл состояния %s: %w", progressPath, err)
					return
				}

				fmt.Printf("Завершено: %s\n", meta.FileName)
				return
			}

			errCh <- fmt.Errorf(
				"загрузка %s завершена не полностью: downloaded=%d/%d, файл состояния сохранён в %s",
				meta.FileName,
				calcDownloadedBytes(chunks, state.DownloadedChunks),
				meta.FileSize,
				progressPath,
			)
		}(rawURL)
	}

	wg.Wait()
	p.Wait()
	close(errCh)

	hadErrors := false
	for err := range errCh {
		if err != nil {
			hadErrors = true
			fmt.Printf("Ошибка: %v\n", err)
		}
	}

	if hadErrors {
		fmt.Println("Загрузка завершена с ошибками.")
	} else {
		fmt.Println("Все файлы успешно загружены!")
	}
}
