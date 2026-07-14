//go:build !yandexdev

package main

import (
	"fmt"
	"net/http"
)

func isYandexDiskPublicURL(raw string) bool {
	return false
}

func resolveYandexPublicDownloadURL(client *http.Client, publicKey string) (string, error) {
	return "", fmt.Errorf("поддержка Яндекс.Диска отключена в этой сборке")
}
