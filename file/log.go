package file

import "os"

// LogFile
type LogFile struct {
	f *os.File
}

// Close
func (lf *LogFile) Close() error {
	if err := lf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (lf *LogFile) Write(bytes []byte) error {
	return nil
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
