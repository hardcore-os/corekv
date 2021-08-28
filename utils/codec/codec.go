package codec

import "encoding/json"

// WalCodec 写入wal文件的编码
func WalCodec(entry *Entry) []byte {
	data, _ := json.Marshal(entry)
	return data
}

func ValuePtrCodec(ptr *ValuePtr) []byte {
	return []byte{}
}
