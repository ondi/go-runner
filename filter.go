//
//
//

package runner

import (
	"sync"
	"time"

	cache "github.com/ondi/go-ttl-cache"
)

type Repack interface {
	Len() int
	IdAdd(i int) string
	IdDel(i int) string
	Swap(i int, j int)
}

type FilterKey_t struct {
	Entry Entry_t
	Id    string
}

type Filter_t struct {
	mx sync.Mutex
	cx *cache.Cache_t[FilterKey_t, struct{}]
}

func NewFilter(filter_size int, filter_ttl time.Duration) (self *Filter_t) {
	self = &Filter_t{
		cx: cache.New(filter_size, filter_ttl, cache.Drop[FilterKey_t, struct{}]),
	}
	return self
}

func (self *Filter_t) Add(ts time.Time, entry Entry_t, in Repack, length int) (added int) {
	var ok bool
	self.mx.Lock()
	for added < length {
		if _, ok = self.cx.Create(
			ts,
			FilterKey_t{Entry: entry, Id: in.IdAdd(added)},
			func(*struct{}) {},
			func(*struct{}) {},
		); ok {
			added++
		} else {
			length--
			in.Swap(added, length)
		}
	}
	self.mx.Unlock()
	return
}

func (self *Filter_t) Del(ts time.Time, entry Entry_t, in Repack) (removed int) {
	var ok bool
	pack_len := in.Len()
	self.mx.Lock()
	for i := 0; i < pack_len; i++ {
		if _, ok = self.cx.Remove(
			ts,
			FilterKey_t{Entry: entry, Id: in.IdDel(i)},
		); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Filter_t) Range(ts time.Time, fn func(key FilterKey_t, value struct{}) bool) {
	self.mx.Lock()
	self.cx.Range(ts, fn)
	self.mx.Unlock()
}

func (self *Filter_t) Size(ts time.Time) (res int) {
	self.mx.Lock()
	res = self.cx.Size(ts)
	self.mx.Unlock()
	return
}

func ThinOut(len_in, len_out int, fn func(p int)) {
	step := len_in / len_out
	rest := len_in - len_out*step
	for i := 0; i < len_in; i += step {
		fn((i + i + step) / 2)
		if rest > 0 {
			rest--
			i++
		}
	}
	return
}
