package kafka

import (
	"fmt"
	"log"
)

type Logger interface {
	Fatalf(string, ...interface{})
	Panicf(string, ...interface{})
	Errorf(string, ...interface{})
	Warnf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

const loggingPrefix = "go-kafka"

type nullLogger struct{}

func (l nullLogger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(fmt.Sprintf("%s [FATAL]: %s", loggingPrefix, format), v...)
}
func (l nullLogger) Panicf(format string, v ...interface{}) {
	log.Panicf(fmt.Sprintf("%s [PANIC]: %s", loggingPrefix, format), v...)
}

func (l nullLogger) Errorf(format string, v ...interface{}) {}
func (l nullLogger) Warnf(format string, v ...interface{})  {}
func (l nullLogger) Infof(format string, v ...interface{})  {}
func (l nullLogger) Debugf(format string, v ...interface{}) {}

// stdDebugLogger logs to stdout up to the `DebugF` level
type stdDebugLogger struct{}

func (l stdDebugLogger) Fatalf(format string, v ...interface{}) {
	log.Fatalf(fmt.Sprintf("%s [FATAL]: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Panicf(format string, v ...interface{}) {
	log.Panicf(fmt.Sprintf("%s [PANIC]: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Errorf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s [ERROR]: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Warnf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s [WARN]: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Infof(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s [INFO]: %s", loggingPrefix, format), v...)
}

func (l stdDebugLogger) Debugf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s [DEBUG]: %s", loggingPrefix, format), v...)
}
