package file

import "github.com/hardcore-os/corekv/utils/codec"

type WalFile struct {
	f *MockFile
}

// WalFile
func (wf *WalFile) Close() error {
	if err := wf.f.Close(); err != nil {
		return err
	}
	return nil
}
func OpenWalFile(opt *Options) *WalFile { return &WalFile{f: OpenMockFile(opt)} }

func (wf *WalFile) Write(entry *codec.Entry) error {
	// 落预写日志简单的同步写即可
	// 序列化为磁盘结构
	walData := codec.WalCodec(entry)
	_, err := wf.f.Write(walData)
	return err
}
