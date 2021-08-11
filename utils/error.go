package utils

// Panic 如果err 不为nil 则panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
