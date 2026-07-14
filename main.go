package main

import (
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

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

const chunkSize = 10 * 1024 * 1024 // 10 MB

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

	if isYandexDiskPublicURL(rawURL) {
		href, err := resolveYandexPublicDownloadURL(client, rawURL)
		if err != nil {
			return FileMeta{}, err
		}
		downloadURL = href
	}

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

func downloadFile(client *http.Client, url, savePaths string) error {
	if err := os.MkdirAll(savePaths, os.ModePerm); err != nil {
		return fmt.Errorf("не удалось создать директорию: %w", err)
	}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("сервер вернул %d", resp.StatusCode)
	}

	filename := path.Base(resp.Request.URL.Path)
	if filename == "." || filename == "/" || filename == "" {
		return fmt.Errorf("не удалось определить имя файла из URL")
	}

	fullPath := filepath.Join(savePaths, filename)

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err = io.Copy(file, resp.Body); err != nil {
		return err
	}

	fmt.Printf("Файл сохранён: %s\n", fullPath)

	return nil
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

	wg := sync.WaitGroup{}

	for _, rawUrl := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			meta, err := FetchMetaData(client, u)
			if err != nil {
				fmt.Printf("Ошибка для %s: %v\n", u, err)
				return
			}

			fullPath := path.Join(savePath, meta.FileName)

			file, err := createSparseFile(fullPath, meta.FileSize)
			if err != nil {
				fmt.Printf("Ошибка подготовки файла %s: %v\n", meta.FileName, err)
				return
			}
			defer file.Close()

			chunks := buildChunks(meta.FileSize, chunkSize)

			fmt.Printf("Файл: %s (%d байт)\n", meta.FileName, meta.FileSize)
			fmt.Printf("Размер чанка: %d байт\n", chunkSize)
			fmt.Printf("Количество чанков: %d\n", len(chunks))

			if meta.AcceptRanges {
				fmt.Println("  Докачка: поддерживается")
				fmt.Printf("Начало загрузки...")
			} else {
				fmt.Println("  Докачка: не поддерживается")
				fmt.Printf("Начало загрузки целиком...")
			}

			fmt.Println()

			for _, chunk := range chunks {
				fmt.Printf(
					"Чанк %d/%d: байты %d-%d\n",
					chunk.Index+1,
					len(chunks),
					chunk.Start,
					chunk.End,
				)
			}

			//if err = downloadFile(client, u, savePath); err != nil {
			//	fmt.Printf("Ошибка: %v\n", err)
			//	return
			//}
			//
			//fmt.Printf("Завершено: %s\n", name)
		}(rawUrl)
	}

	wg.Wait()
	fmt.Println("Все файлы загружены!")

}
