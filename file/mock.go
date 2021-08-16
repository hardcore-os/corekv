package file

import (
	"fmt"
	"os"

	"github.com/hardcore-os/corekv/utils"
)

// MockFile
type MockFile struct {
	f   *os.File
	opt *Options
}

// Close
func (lf *MockFile) Close() error {
	if err := lf.f.Close(); err != nil {
		return err
	}
	return nil
}

func (lf *MockFile) Write(bytes []byte) (int, error) {
	return lf.f.Write(bytes)
}
func (lf *MockFile) Read(bytes []byte) (int, error) {
	return lf.f.Read(bytes)
}

// Truncature 截断
func (lf *MockFile) Truncature(n int64) error {
	return lf.f.Truncate(n)
}

// ReName 重命名
func (lf *MockFile) ReName(name string) error {
	err := os.Rename(fmt.Sprintf("%s/%s", lf.opt.Dir, lf.opt.Name), fmt.Sprintf("%s/%s", lf.opt.Dir, name))
	lf.opt.Name = name
	return err
}

// Options
type Options struct {
	Name string
	Dir  string
}

// OpenMockFile mock 文件
func OpenMockFile(opt *Options) *MockFile {
	var err error
	lf := &MockFile{}
	lf.opt = opt
	lf.f, err = os.OpenFile(fmt.Sprintf("%s/%s", opt.Dir, opt.Name), os.O_CREATE|os.O_RDWR, 0666)
	utils.Panic(err)
	return lf
}
