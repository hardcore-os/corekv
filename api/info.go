package api

type Infos struct {
}

func (db *DB) Info() *Infos {
	return &Infos{}
}
