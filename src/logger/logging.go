package logger

import (
	"github.com/op/go-logging"
	"log"
	"os"
	"time"
)

var Logger *logging.Logger
var LogFile *os.File
var debugLogfile *os.File
var debugstdout = false


func SetLogger(logName string){
	now := time.Now().Format("2006-01-02 15:04:05")
	var err error
	LogFile,err = os.OpenFile(logName+ "-" + now + "-info.log",os.O_APPEND|os.O_WRONLY|os.O_CREATE,0666)
	debugLogfile,err = os.OpenFile(logName+ "-" + now + "-debug.log",os.O_APPEND|os.O_WRONLY|os.O_CREATE,0666)
	if err != nil{
		log.Fatalf(err.Error())
	}
	var debugBackend *logging.LogBackend
	var debugformat logging.Formatter
	if debugstdout{
		debugBackend = logging.NewLogBackend(os.Stdout,"",0)
	}else{
		debugBackend = logging.NewLogBackend(debugLogfile,"",0)
	}
	InfoBackend := logging.NewLogBackend(LogFile,"",0)

	if debugstdout{
		debugformat = logging.MustStringFormatter(`%{color:reset}[%{level:.5s}] %{time:15:04:05} %{shortfile}  %{message}`)
	}else{
		debugformat = logging.MustStringFormatter(`[%{level:.5s}] %{time:15:04:05} %{shortfile} %{callpath:1}: %{message}`)
	}
	infoformat := logging.MustStringFormatter(`[%{level:.5s}] %{time:15:04:05}  %{shortfile} %{callpath:1}: %{message}`)

	debugbandf := logging.NewBackendFormatter(debugBackend,debugformat)
	infobandf := logging.NewBackendFormatter(InfoBackend,infoformat)

	backend1level := logging.AddModuleLevel(debugbandf)
	backend1level.SetLevel(logging.DEBUG,"")
	backend2level := logging.AddModuleLevel(infobandf)
	backend2level.SetLevel(logging.INFO,"")
	logging.SetBackend(backend1level,backend2level)
	Logger = logging.MustGetLogger("main")
}

func GetLogger(logName string) *logging.Logger{
	if Logger == nil{
		SetLogger(logName)
	}
	return Logger
}

//func Debugf(format string, args ...interface{}) {
//	Logger.Debugf(format,args)
//}
//
//func Infof(format string, args ...interface{}) {
//	Logger.Infof(format,args)
//}
//
//func Warnf(format string, args ...interface{}) {
//	Logger.Warningf(format,args)
//}
//func Errorf(format string, args ...interface{}) {
//	Logger.Errorf(format,args)
//}
//func Fatalf(format string, args ...interface{}) {
//	Logger.Fatalf(format,args)
//}