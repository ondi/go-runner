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

type Pack interface {
	Len() int
}

type PackID interface {
	Pack
	IDString(i int) string
}

type Repack interface {
	PackID
	Swap(i int, j int)
	Resize(i int)
}

type Name interface {
	ServiceName() string
}

type Service interface {
	Name
	ServiceDo(msg Pack)
}

type Runner interface {
	RunRepack(ts time.Time, service Service, packs ...Repack) (total int, last int)
	RunPack(service Service, packs ...Pack) (last int)
	Remove(ts time.Time, name Name, pack PackID) (removed int)
	Running() int64
	SizeFilter(ts time.Time) int
	SizeQueue() int
	FlushFilter(ts time.Time)
	Close()
}

type msg_t struct {
	service Service
	pack    Pack
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

func (self *Runner_t) __repack(ts time.Time, service Service, pack Repack) (i int) {
	var ok bool
	last := pack.Len() - 1
	for i <= last {
		if _, ok = self.cx.Push(
			ts,
			service.ServiceName()+pack.IDString(i),
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

func (self *Runner_t) RunRepack(ts time.Time, service Service, packs ...Repack) (total int, last int) {
	self.mx.Lock()
	any := cap(self.queue) - len(self.queue)
	// repack all before processing
	for any > 0 && last < len(packs) {
		if added := self.__repack(ts, service, packs[last]); added > 0 {
			total += added
			any--
		}
		last++
	}
	for any = 0; any < last; any++ {
		if packs[any].Len() > 0 {
			self.queue <- msg_t{service: service, pack: packs[any]}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunPack(service Service, packs ...Pack) (last int) {
	self.mx.Lock()
	if last = cap(self.queue) - len(self.queue); last > len(packs) {
		last = len(packs)
	}
	for i := 0; i < last; i++ {
		self.queue <- msg_t{service: service, pack: packs[i]}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, name Name, pack PackID) (removed int) {
	self.mx.Lock()
	var ok bool
	for i := 0; i < pack.Len(); i++ {
		if _, ok = self.cx.Remove(ts, name.ServiceName()+pack.IDString(i)); ok {
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
		v.service.ServiceDo(v.pack)
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
