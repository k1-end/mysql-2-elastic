package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a new structured logger with a JSON handler.
func NewLogger() *slog.Logger {
	// Configure the handler to output JSON and include source code location for errors.
	handlerOptions := &slog.HandlerOptions{
		AddSource: true, // Add file and line number
		Level:     slog.LevelDebug, // Set default logging level
	}

	// Create a JSON handler that writes to stderr.
	handler := slog.NewJSONHandler(os.Stderr, handlerOptions)
	return slog.New(handler)
}

type SlogWriter struct {
	logger *slog.Logger
	level  slog.Level // The level at which to log messages from the standard logger
}

// NewSlogWriter creates a new SlogWriter.
func NewSlogWriter(logger *slog.Logger, level slog.Level) *SlogWriter {
	return &SlogWriter{
		logger: logger,
		level:  level,
	}
}

// Write implements the io.Writer interface.
func (sw *SlogWriter) Write(p []byte) (n int, err error) {
	// The standard log package often adds prefixes (e.g., "ELASTIC_INFO: ")
	// and timestamp/file info. slog handles these natively, so you might want to
	// strip them off the incoming message from the standard log.
	msg := strings.TrimSpace(string(p))

	// Optionally, you can try to parse the level from the incoming log message
	// if the standard logger prefix indicates it (e.g., "INFO: ", "ERROR: ").
	// This is more complex if the standard logger only uses generic prefixes.
	// For simplicity, we'll use the pre-defined sw.level.

	sw.logger.Log(context.Background(), sw.level, msg, slog.String("component", "go-elasticsearch"))
	// You might want to parse the message more granularly if you want to
	// extract things like the file/line info that standard log adds and
	// put them as separate slog attributes. This can be tricky.

	return len(p), nil
}

func (m *SlogWriter) Printf(format string, v ...any) {
    m.logger.Info(fmt.Sprintf(format, v...))
}
