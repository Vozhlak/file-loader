package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

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

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			name := path.Base(u)

			resp, err := client.Head(u)
			if err != nil {
				fmt.Printf("Ошибка HEAD-запроса для %s: %v\n", name, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				fmt.Printf("Ошибка: сервер вернул %d для %s\n", resp.StatusCode, name)
				return
			}

			fmt.Printf("Файл: %s\n", name)

			contentLength := resp.Header.Get("Content-Length")
			size, err := strconv.ParseInt(contentLength, 10, 64)
			if err != nil || size <= 0 {
				fmt.Println("  Размер: неизвестен")
			} else {
				fmt.Printf("  Размер: %d байт\n", size)
			}

			acceptRanges := resp.Header.Get("Accept-Ranges")
			supportResume := acceptRanges == "bytes"

			if supportResume {
				fmt.Println("  Докачка: поддерживается")
				fmt.Printf("Начало загрузки...")
			} else {
				fmt.Println("  Докачка: не поддерживается")
				fmt.Printf("Начало загрузки целиком...")
			}

			if err = downloadFile(client, u, savePath); err != nil {
				fmt.Printf("Ошибка: %v\n", err)
				return
			}

			fmt.Printf("Завершено: %s\n", name)
		}(url)
	}

	wg.Wait()
	fmt.Println("Все файлы загружены!")

}
