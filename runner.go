//
//
//

package runner

import (
	"sync"
	"time"

	cache "github.com/ondi/go-ttl-cache"
)

type PackID interface {
	Len() int
	IDString(i int) string
}

type Repack interface {
	PackID
	Swap(i int, j int)
	Resize(i int)
}

type Result interface {
	Total(int)
}

type Call func(out Result, in PackID)

type msg_t struct {
	service  string
	function string
	fn       Call
	in       Repack
	out      Result
}

type key_t struct {
	service string
	id      string
}

type Runner_t struct {
	mx         sync.Mutex
	cx         *cache.Cache_t
	queue      chan msg_t
	queue_size int
	services   map[string]int
	functions  map[string]int
	wg         sync.WaitGroup
}

func New(threads int, queue_size int, filter_size int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		queue:      make(chan msg_t, queue_size),
		services:   map[string]int{},
		functions:  map[string]int{},
		queue_size: queue_size,
	}
	self.cx = cache.New(filter_size, filter_ttl, cache.Drop)
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, service string, in Repack) (added int) {
	var ok bool
	length := in.Len()
	for added < length {
		_, ok = self.cx.Create(
			ts,
			key_t{service: service, id: in.IDString(added)},
			func() interface{} { return nil },
			func(prev interface{}) interface{} { return prev },
		)
		if ok {
			added++
		} else {
			length--
			in.Swap(added, length)
		}
	}
	in.Resize(added)
	return in.Len()
}

// Total() should be called before start
func (self *Runner_t) __queue(ts time.Time, service string, function string, fn Call, out Result, in []Repack) (queued int) {
	var last, added int
	available := self.queue_size - len(self.queue)
	for ; available > 0 && last < len(in); last++ {
		if added = self.__repack(ts, service, in[last]); added != 0 {
			available--
			queued += added
		}
	}
	out.Total(queued)
	for available = 0; available < last; available++ {
		if in[available].Len() != 0 {
			self.services[service]++
			self.functions[service+"::"+function]++
			self.queue <- msg_t{service: service, function: function, fn: fn, in: in[available], out: out}
		}
	}
	return
}

func (self *Runner_t) RunAny(ts time.Time, service string, function string, fn Call, out Result, in []Repack) (queued int) {
	self.mx.Lock()
	queued = self.__queue(ts, service, function, fn, out, in)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunAnyEx(count int, ts time.Time, service string, function string, fn Call, out Result, in []Repack) (queued int) {
	self.mx.Lock()
	if self.services[service] < count {
		queued = self.__queue(ts, service, function, fn, out, in)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, service string, pack PackID) (removed int) {
	self.mx.Lock()
	for i := pack.Len() - 1; i > -1; i-- {
		if _, ok := self.cx.Remove(ts, key_t{service: service, id: pack.IDString(i)}); ok {
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
		if temp := self.services[v.service]; temp == 1 {
			delete(self.services, v.service)
		} else if temp != 0 {
			self.services[v.service]--
		}
		function := v.service + "::" + v.function
		if temp := self.functions[function]; temp == 1 {
			delete(self.functions, function)
		} else if temp != 0 {
			self.functions[function]--
		}
		self.mx.Unlock()
	}
}

func (self *Runner_t) RangeRunning(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.functions {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFilter(ts time.Time, fn func(key interface{}, value interface{}) bool) {
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
