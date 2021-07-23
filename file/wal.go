package file

type WalFile struct {
	file *LogFile
}

func OpenWalFile(opt *Options) *WalFile { return &WalFile{file: OpenLogFile(opt)} }
