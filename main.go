package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Использование: downloader <директория> <url1> [url2...]")
		os.Exit(1)
	}

	savePaths := os.Args[1]
	urls := os.Args[2:]

	fmt.Printf("Директория для сохранения: %s\n", savePaths)
	fmt.Println("URL для скачивания:")
	for _, url := range urls {
		fmt.Printf("- %s\n", url)
	}
}
