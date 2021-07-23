package file

type SSTable struct {
	f *LogFile
}

func OpenSStable(opt *Options) *SSTable {
	return &SSTable{}
}
