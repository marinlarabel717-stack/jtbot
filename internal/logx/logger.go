package logx

import (
	"log"
	"os"
)

type Logger struct {
	level string
	base  *log.Logger
}

func New(level string) *Logger {
	return &Logger{
		level: level,
		base:  log.New(os.Stdout, "[jtbot-go] ", log.LstdFlags|log.Lmicroseconds),
	}
}

func (l *Logger) Debugf(format string, args ...any) {
	if l.level == "debug" {
		l.base.Printf("DEBUG "+format, args...)
	}
}

func (l *Logger) Infof(format string, args ...any) {
	l.base.Printf("INFO "+format, args...)
}

func (l *Logger) Errorf(format string, args ...any) {
	l.base.Printf("ERROR "+format, args...)
}
