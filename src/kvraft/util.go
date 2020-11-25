package kvraft

import (
	"log"
	"os"
	"sync"
)

type Logger struct {
	Debug   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
}

var once sync.Once

var L *Logger

func Log() *Logger {
	once.Do(func() {
		L = &Logger{
			Debug:   log.New(os.Stdout, "[DEBUG] ", log.Ldate|log.Ltime|log.Lshortfile),
			Info:    log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime),
			Warning: log.New(os.Stdout, "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),
			Error:   log.New(os.Stdout, "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile),
		}
	})
	return L
}
//

