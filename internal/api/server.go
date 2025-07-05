package api

import (
	"log/slog"
	"net/http"
)

func Serve(logger *slog.Logger) {
    http.HandleFunc("/", rootHandler)
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		logger.Error(err.Error())
	}
}
