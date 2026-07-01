package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
)

func downloadFile(url, savePaths string) error {
	if err := os.MkdirAll(savePaths, os.ModePerm); err != nil {
		return fmt.Errorf("не удалось создать директорию: %w", err)
	}

	resp, err := http.Get(url)
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

	wg := sync.WaitGroup{}

	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()

			name := path.Base(u)
			fmt.Printf("Начало загрузки: %s\n", name)

			if err := downloadFile(u, savePath); err != nil {
				fmt.Printf("Ошибка: %v\n", err)
				return
			}

			fmt.Printf("Завершено: %s\n", name)
		}(url)
	}

	wg.Wait()
	fmt.Println("Все файлы загружены!")

}
