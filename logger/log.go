package logger

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var Log = logrus.New()

type DailyLogWriter struct {
	dir      string
	filename string
	file     *os.File
	mu       sync.Mutex
	lastDate string
}

func NewDailyLogWriter(dir, filename string) *DailyLogWriter {
	writer := &DailyLogWriter{
		dir:      dir,
		filename: filename,
	}
	writer.rotateFileIfNeeded()
	go writer.autoRotate()
	return writer
}

func (w *DailyLogWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rotateFileIfNeeded()
	if w.file == nil {
		return 0, fmt.Errorf("log file not ready")
	}
	return w.file.Write(p)
}

func (w *DailyLogWriter) rotateFileIfNeeded() {
	today := time.Now().Format("2006-01-02")
	if today == w.lastDate && w.file != nil {
		return
	}

	if w.file != nil {
		_ = w.file.Close()
	}

	_ = os.MkdirAll(w.dir, os.ModePerm)
	fullPath := filepath.Join(w.dir, fmt.Sprintf("%s-%s.log", w.filename, today))

	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		w.file = nil
		return
	}

	w.file = file
	w.lastDate = today
}

func (w *DailyLogWriter) autoRotate() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		w.mu.Lock()
		w.rotateFileIfNeeded()
		w.mu.Unlock()
	}
}

func InitLogger() {
	writer := NewDailyLogWriter("logs", "server")

	Log.SetOutput(io.MultiWriter(writer, os.Stdout))

	Log.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	env := os.Getenv("ENV")
	if env == "dev" {
		Log.SetLevel(logrus.DebugLevel)
	} else {
		Log.SetLevel(logrus.InfoLevel)
	}
}
