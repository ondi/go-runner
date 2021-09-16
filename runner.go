//
//
//

package runner

import (
	"sync"
	"sync/atomic"
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

type Runner interface {
	RunRepack(ts time.Time, name string, fn func(in interface{}), packs ...Repack) (added int, passed int, last int)
	RunPack(name string, fn func(in interface{}), packs ...interface{}) (last int)
	Remove(ts time.Time, name string, pack PackID) (removed int)
	Running() int64
	SizeFilter(ts time.Time) int
	SizeQueue() int
	FlushFilter(ts time.Time)
	Close()
}

type msg_t struct {
	fn   func(in interface{})
	pack interface{}
	name string
}

type Runner_t struct {
	mx      sync.Mutex
	cx      *cache.Cache_t
	queue   chan msg_t
	running int64
	wg      sync.WaitGroup
}

func New(threads int, queue int, filter_limit int, filter_ttl time.Duration) Runner {
	self := &Runner_t{
		cx:    cache.New(filter_limit, filter_ttl, cache.Drop),
		queue: make(chan msg_t, queue),
	}
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, name string, pack Repack) (i int) {
	var ok bool
	last := pack.Len() - 1
	for i <= last {
		if _, ok = self.cx.Create(
			ts,
			name+pack.IDString(i),
			func() interface{} { return nil },
			func(interface{}) interface{} { return nil },
		); ok {
			i++
		} else {
			pack.Swap(i, last)
			last--
		}
	}
	pack.Resize(i)
	return
}

func (self *Runner_t) RunRepack(ts time.Time, name string, fn func(in interface{}), packs ...Repack) (added int, passed int, last int) {
	self.mx.Lock()
	any := cap(self.queue) - len(self.queue)
	// repack all before processing
	for any > 0 && last < len(packs) {
		passed += packs[last].Len()
		if add := self.__repack(ts, name, packs[last]); add > 0 {
			added += add
			any--
		}
		last++
	}
	for any = 0; any < last; any++ {
		if packs[any].Len() > 0 {
			self.queue <- msg_t{name: name, fn: fn, pack: packs[any]}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunPack(name string, fn func(in interface{}), packs ...interface{}) (last int) {
	self.mx.Lock()
	if last = cap(self.queue) - len(self.queue); last > len(packs) {
		last = len(packs)
	}
	for i := 0; i < last; i++ {
		self.queue <- msg_t{name: name, fn: fn, pack: packs[i]}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, name string, pack PackID) (removed int) {
	self.mx.Lock()
	var ok bool
	for i := 0; i < pack.Len(); i++ {
		if _, ok = self.cx.Remove(ts, name+pack.IDString(i)); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.queue {
		atomic.AddInt64(&self.running, 1)
		v.fn(v.pack)
		atomic.AddInt64(&self.running, -1)
	}
}

func (self *Runner_t) Running() int64 {
	return atomic.LoadInt64(&self.running)
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

func (self *Runner_t) FlushFilter(ts time.Time) {
	self.mx.Lock()
	self.cx.FlushLimit(ts, 0)
	self.mx.Unlock()
}

func (self *Runner_t) Close() {
	close(self.queue)
	self.wg.Wait()
}
