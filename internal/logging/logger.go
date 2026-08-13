package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

func NewLogger(level, format string) *slog.Logger {
	return NewLoggerTo(os.Stderr, level, format)
}

func NewLoggerTo(w io.Writer, level, format string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: l}
	if strings.EqualFold(format, "json") {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
