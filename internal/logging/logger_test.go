package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger("info", "text")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewLoggerToLogLevels(t *testing.T) {
	tests := []struct {
		level     string
		logFunc   func(buf *bytes.Buffer)
		shouldLog bool
	}{
		{
			level: "debug",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "debug", "text")
				logger.Debug("debug message")
			},
			shouldLog: true,
		},
		{
			level: "info",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "info", "text")
				logger.Debug("debug message should be suppressed")
			},
			shouldLog: false,
		},
		{
			level: "warn",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "warn", "text")
				logger.Warn("warn message")
			},
			shouldLog: true,
		},
		{
			level: "warning",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "warning", "text")
				logger.Warn("warning message")
			},
			shouldLog: true,
		},
		{
			level: "error",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "error", "text")
				logger.Info("info message should be suppressed")
				logger.Error("error message")
			},
			shouldLog: true,
		},
		{
			level: "default_fallback",
			logFunc: func(buf *bytes.Buffer) {
				logger := NewLoggerTo(buf, "unknown_level", "text")
				logger.Info("info message on default fallback")
			},
			shouldLog: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			var buf bytes.Buffer
			tt.logFunc(&buf)
			hasOutput := buf.Len() > 0
			if hasOutput != tt.shouldLog {
				t.Fatalf("level %q: got output=%v, want output=%v, buf content: %q", tt.level, hasOutput, tt.shouldLog, buf.String())
			}
		})
	}
}

func TestNewLoggerToJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerTo(&buf, "info", "json")
	logger.Info("service started", "service_name", "async-trace-doctor", "port", 8080)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("expected valid JSON log output, got error: %v, raw: %s", err, buf.String())
	}

	if entry["msg"] != "service started" {
		t.Errorf("expected msg=%q, got %v", "service started", entry["msg"])
	}
	if entry["service_name"] != "async-trace-doctor" {
		t.Errorf("expected service_name=%q, got %v", "async-trace-doctor", entry["service_name"])
	}
	if entry["level"] != "INFO" {
		t.Errorf("expected level=INFO, got %v", entry["level"])
	}
}

func TestNewLoggerToTextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerTo(&buf, "info", "text")
	logger.Info("audit completed", "violations", 3)

	out := buf.String()
	if !strings.Contains(out, "audit completed") || !strings.Contains(out, "violations=3") {
		t.Fatalf("expected text formatted log to contain message and attributes, got: %q", out)
	}
}
