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

type Call func(r Result, in interface{})

type msg_t struct {
	name string
	fn   Call
	r    Result
	pack interface{}
}

type Runner_t struct {
	mx         sync.Mutex
	cx         *cache.Cache_t
	queue      chan msg_t
	queue_size int
	running    map[string]int
	wg         sync.WaitGroup
}

type filter_key struct {
	name string
	id   string
}

func New(threads int, queue_size int, filter_size int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		queue:      make(chan msg_t, queue_size),
		running:    map[string]int{},
		queue_size: queue_size,
	}
	self.cx = cache.New(filter_size, filter_ttl, cache.Drop)
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, name string, pack Repack) (added int) {
	var ok bool
	last := pack.Len()
	for added < last {
		if _, ok = self.cx.Create(
			ts,
			filter_key{name: name, id: pack.IDString(added)},
			func() interface{} { return nil },
			func(prev interface{}) interface{} { return prev },
		); ok {
			added++
		} else {
			last--
			pack.Swap(added, last)
		}
	}
	pack.Resize(added)
	return
}

// Total() should be called before processing
func (self *Runner_t) __queue_repack(ts time.Time, name string, fn Call, r Result, packs []Repack) (input int, queued int) {
	var last, added int
	available := self.queue_size - len(self.queue)
	for ; available > 0 && last < len(packs); last++ {
		input += packs[last].Len()
		if added = self.__repack(ts, name, packs[last]); added > 0 {
			queued += added
			available--
		}
	}
	r.Total(queued)
	for available = 0; available < last; available++ {
		if packs[available].Len() > 0 {
			self.running[name]++
			self.queue <- msg_t{name: name, fn: fn, r: r, pack: packs[available]}
		}
	}
	return
}

// Total() should be called before processing
func (self *Runner_t) __queue_all(ts time.Time, name string, fn Call, r Result, packs []interface{}) (input int, queued int) {
	input = len(packs)
	queued = self.queue_size - len(self.queue)
	if queued < input {
		input = queued
	} else {
		queued = input
	}
	r.Total(queued)
	for queued = 0; queued < input; queued++ {
		self.running[name]++
		self.queue <- msg_t{name: name, fn: fn, r: r, pack: packs[queued]}
	}
	return
}

func (self *Runner_t) RunRepack(ts time.Time, name string, fn Call, r Result, packs []Repack) (input int, queued int) {
	self.mx.Lock()
	input, queued = self.__queue_repack(ts, name, fn, r, packs)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunAll(ts time.Time, name string, fn Call, r Result, packs []interface{}) (input int, queued int) {
	self.mx.Lock()
	if self.running[name] > 0 {
		self.mx.Unlock()
		return
	}
	input, queued = self.__queue_all(ts, name, fn, r, packs)
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, name string, pack PackID) (removed int) {
	var ok bool
	self.mx.Lock()
	for i := 0; i < pack.Len(); i++ {
		if _, ok = self.cx.Remove(ts, filter_key{name: name, id: pack.IDString(i)}); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.queue {
		v.fn(v.r, v.pack)
		self.mx.Lock()
		if temp, ok := self.running[v.name]; temp == 1 {
			delete(self.running, v.name)
		} else if ok {
			self.running[v.name]--
		}
		self.mx.Unlock()
	}
}

func (self *Runner_t) RangeRunning(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.running {
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
