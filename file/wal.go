package file

import "github.com/hardcore-os/corekv/codec"

type WalFile struct {
	f *LogFile
}

// WalFile
func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}
func OpenWalFile(opt *Options) *WalFile { return &WalFile{f: OpenLogFile(opt)} }

func (wf *WalFile) Write(entry *codec.Entry) error {
	// 落预写日志简单的同步写即可
	// 序列化为磁盘结构
	walData := codec.WalCodec(entry)
	return wf.f.Write(walData)
}
