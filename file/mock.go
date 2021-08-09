package file

import "os"

// MockFile
type MockFile struct {
	f *os.File
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

// Options
type Options struct {
	Name string
}

// OpenMockFile
func OpenMockFile(opt *Options) *MockFile {
	lf := &MockFile{}
	lf.f, _ = os.Create(opt.Name)
	return lf
}
