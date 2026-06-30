package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
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

	fullPath := path.Join(savePaths, filename)

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
	url := os.Args[2]

	fmt.Printf("Скачивание: %s\n", url)

	if err := downloadFile(url, savePath); err != nil {
		fmt.Printf("Ошибка: %v\n", err)
		os.Exit(1)
	}
}
