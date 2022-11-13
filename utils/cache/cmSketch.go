package cache

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}

	// numCounters 一定是二次幂，也就一定是1后面有 n 个 0
	numCounters = next2Power(numCounters)
	// mask 一定是0111...111
	sketch := &cmSketch{
		mask: uint64(numCounters - 1),
	}

	source := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}

	return sketch
}

func (s *cmSketch) Increment(hashed uint64) {
	for i := 0; i < cmDepth; i++ {
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := 0; i < cmDepth; i++ {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}
	return int64(min)
}

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

// 快速计算大于 X，且最接近 X 的二次幂
func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make([]byte, numCounters/2)
}

func (r cmRow) get(n uint64) byte {
	return byte(r[n>>1]>>((n&1)*4)) & 0x0f
}

func (r cmRow) increment(n uint64) {
	i := n / 2
	v := byte(r[i]>>((n&1)*4)) & 0x0f
	k := (n & 1) * 4

	if v < 15 {
		r[i] = r[i] + (1 << k)
	}
}

func (r cmRow) reset() {
	for i := 0; i < len(r); i++ {
		r[i] = (r[i] >> 1) & 0x77
	}
}

func (r cmRow) clear() {
	for i := 0; i < len(r); i++ {
		r[i] = 0
	}
}

func (r cmRow) string() string {
	s := ""
	for i := uint64(0); i < uint64(len(r)*2); i++ {
		s += fmt.Sprintf("%02d ", (r[(i/2)]>>((i&1)*4))&0x0f)
	}
	s = s[:len(s)-1]
	return s
}
