//
//
//

package runner

import (
	"sync"
	"time"

	cache "github.com/ondi/go-ttl-cache"
)

type key_t struct {
	Entry Entry_t
	Id    string
}

type Filter_t struct {
	mx sync.Mutex
	cx *cache.Cache_t[key_t, struct{}]
}

func NewFilter(filter_size int, filter_ttl time.Duration) (self *Filter_t) {
	self = &Filter_t{
		cx: cache.New(filter_size, filter_ttl, cache.Drop[key_t, struct{}]),
	}
	return self
}

func (self *Filter_t) Add(ts time.Time, entry Entry_t, in Repack, length int) (added int) {
	var ok bool
	self.mx.Lock()
	for added < length {
		_, ok = self.cx.Create(
			ts,
			key_t{Entry: entry, Id: in.IDString(added)},
			func(*struct{}) {},
			func(*struct{}) {},
		)
		if ok {
			added++
		} else {
			length--
			in.Swap(added, length)
		}
	}
	self.mx.Unlock()
	return
}

func (self *Filter_t) Del(ts time.Time, entry Entry_t, pack Repack) (removed int) {
	var ok bool
	pack_len := pack.Len()
	self.mx.Lock()
	for i := 0; i < pack_len; i++ {
		if _, ok = self.cx.Remove(ts, key_t{Entry: entry, Id: pack.IDString(i)}); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Filter_t) Range(ts time.Time, fn func(key key_t, value struct{}) bool) {
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

func ThinOut(in_len, out_len int) (out []int) {
	part_size := in_len / out_len
	rest := in_len - out_len*part_size
	for i := 0; i < in_len; i += part_size {
		out = append(out, (i+i+part_size)/2)
		if rest > 0 {
			i++
			rest--
		}
	}
	return
}
