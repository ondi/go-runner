//
//
//

package runner

import (
	"sync"
	"time"

	cache "github.com/ondi/go-ttl-cache"
)

type Pack interface {
	Len() int
	IDString(i int) string
}

type Repack interface {
	Pack
	Swap(i int, j int)
	Resize(i int)
}

type Entry_t struct {
	Service  string
	Function string
}

type Result interface {
	Total(int)
}

type Call func(out Result, in Pack) (err error)

type msg_t struct {
	entry Entry_t
	fn    Call
	in    Repack
	out   Result
}

type FilterKey_t struct {
	Service string
	Id      string
}

type Runner_t struct {
	mx         sync.Mutex
	cx         *cache.Cache_t[FilterKey_t, struct{}]
	queue      chan msg_t
	queue_size int
	services   map[string]int
	functions  map[Entry_t]int
	wg         sync.WaitGroup
}

func New(threads int, queue_size int, filter_size int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		queue:      make(chan msg_t, queue_size),
		services:   map[string]int{},
		functions:  map[Entry_t]int{},
		queue_size: queue_size,
	}
	self.cx = cache.New(filter_size, filter_ttl, cache.Drop[FilterKey_t, struct{}])
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, service string, in Repack, length int) (added int) {
	var ok bool
	for added < length {
		_, ok = self.cx.Create(
			ts,
			FilterKey_t{Service: service, Id: in.IDString(added)},
			func() struct{} { return struct{}{} },
			func(*struct{}) {},
		)
		if ok {
			added++
		} else {
			length--
			in.Swap(added, length)
		}
	}
	in.Resize(added)
	return
}

// Total() should be called before start
func (self *Runner_t) __queue(ts time.Time, entry Entry_t, fn Call, out Result, in []Repack) (input int, queued int) {
	var last, added int
	available := self.queue_size - len(self.queue)
	for ; available > 0 && last < len(in); last++ {
		added = in[last].Len()
		input += added
		self.__repack(ts, entry.Service, in[last], added)
		if added = in[last].Len(); added != 0 {
			available--
			queued += added
		}
	}
	out.Total(queued)
	for available = 0; available < last; available++ {
		if in[available].Len() != 0 {
			self.services[entry.Service]++
			self.functions[entry]++
			self.queue <- msg_t{entry: entry, fn: fn, in: in[available], out: out}
		}
	}
	return
}

func (self *Runner_t) RunAny(ts time.Time, entry Entry_t, fn Call, out Result, in []Repack) (input int, queued int) {
	self.mx.Lock()
	input, queued = self.__queue(ts, entry, fn, out, in)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunAnySrv(count int, ts time.Time, entry Entry_t, fn Call, out Result, in []Repack) (input int, queued int) {
	self.mx.Lock()
	if self.services[entry.Service] < count {
		input, queued = self.__queue(ts, entry, fn, out, in)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunAnyFun(count int, ts time.Time, entry Entry_t, fn Call, out Result, in []Repack) (input int, queued int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		input, queued = self.__queue(ts, entry, fn, out, in)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, service string, pack Pack) (removed int) {
	self.mx.Lock()
	for i := pack.Len() - 1; i > -1; i-- {
		if _, ok := self.cx.Remove(ts, FilterKey_t{Service: service, Id: pack.IDString(i)}); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.queue {
		v.fn(v.out, v.in)
		self.mx.Lock()
		if temp := self.services[v.entry.Service]; temp == 1 {
			delete(self.services, v.entry.Service)
		} else if temp != 0 {
			self.services[v.entry.Service]--
		}
		if temp := self.functions[v.entry]; temp == 1 {
			delete(self.functions, v.entry)
		} else if temp != 0 {
			self.functions[v.entry]--
		}
		self.mx.Unlock()
	}
}

func (self *Runner_t) RangeSv(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.services {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFn(fn func(key Entry_t, value int) bool) {
	self.mx.Lock()
	for k, v := range self.functions {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFilter(ts time.Time, fn func(key FilterKey_t, value struct{}) bool) {
	self.mx.Lock()
	self.cx.Range(ts, fn)
	self.mx.Unlock()
}

func (self *Runner_t) SizeFilter(ts time.Time) (res int) {
	self.mx.Lock()
	res = self.cx.Size(ts)
	self.mx.Unlock()
	return
}

func (self *Runner_t) SizeQueue() int {
	return len(self.queue)
}

func (self *Runner_t) Close() {
	self.mx.Lock()
	self.queue_size = 0
	self.mx.Unlock()
	close(self.queue)
	self.wg.Wait()
}
