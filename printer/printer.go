package printer

import "fmt"

type Printer interface {
	Printf(string, ...interface{})
}

type LogPrinter struct{}

func (LogPrinter) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
