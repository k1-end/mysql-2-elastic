package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a structured JSON logger writing to stderr.
func NewLogger(level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
	})
	return slog.New(handler)
}

// SlogWriter adapts an slog.Logger to io.Writer and provides Printf.
type SlogWriter struct {
	logger *slog.Logger
	level  slog.Level
}

// NewSlogWriter creates a new SlogWriter.
func NewSlogWriter(logger *slog.Logger, level slog.Level) *SlogWriter {
	return &SlogWriter{logger: logger, level: level}
}

// Write implements io.Writer.
func (sw *SlogWriter) Write(p []byte) (int, error) {
	sw.logger.Log(context.Background(), sw.level, strings.TrimSpace(string(p)), slog.String("component", "go-elasticsearch"))
	return len(p), nil
}

// Printf implements a Printf-style interface.
func (sw *SlogWriter) Printf(format string, v ...any) {
	sw.logger.Info(format, "args", v)
}
