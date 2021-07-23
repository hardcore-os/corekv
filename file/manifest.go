package file

type Manifest struct {
	f *LogFile
}

func OpenManifest(opt *Options) *Manifest {
	return &Manifest{}
}
