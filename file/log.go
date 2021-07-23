package file

import "os"

// LogFile
type LogFile struct {
	f *os.File
}

// Options
type Options struct {
	name string
}

// OpenLogFile
func OpenLogFile(opt *Options) *LogFile {
	lf := &LogFile{}
	lf.f, _ = os.Create(opt.name)
	return lf
}
