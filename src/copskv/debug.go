/* debug.go
 DPrintf & related constants/variables
 */
package copskv

import (
	"log"
	"os"
)


const Debug = 0
var logFile *os.File = nil

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		if logFile == nil {
			var err error
			logFile, err = os.OpenFile("debug-logs.txt", os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0666)
			if err != nil {
				log.Fatal("error opening logFile", err)
			}
			log.SetOutput(logFile)
		}
		log.Printf(format, a...)
	}
	return
}
