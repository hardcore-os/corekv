package file

type SSTable struct {
	f *MockFile
}

func OpenSStable(opt *Options) *SSTable {
	return &SSTable{}
}
