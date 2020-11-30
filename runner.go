//
//
//

package runner

import (
	"fmt"
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
	RunAll(ts time.Time, service Service, packs ...Repack) (total int, err error)
	RunPartial(ts time.Time, service Service, packs ...Repack) (total int, last int)
	RunSimple(service Service, pack Pack) (err error)
	Remove(ts time.Time, name Name, pack PackID) (removed int)
	Running() int64
	SizeFilter(ts time.Time) int
	SizeQueue() int
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

func New(threads int, queue int, limit int, ttl time.Duration) (self *Runner_t) {
	self = &Runner_t{
		cx:    cache.New(limit, ttl, cache.Drop),
		queue: make(chan msg_t, queue),
	}
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return
}

func (self *Runner_t) __filter(ts time.Time, service Service, pack Repack) (i int) {
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

func (self *Runner_t) RunAll(ts time.Time, service Service, packs ...Repack) (total int, err error) {
	self.mx.Lock()
	if len(packs) > cap(self.queue)-len(self.queue) {
		self.mx.Unlock()
		err = fmt.Errorf("OVERFLOW")
		return
	}
	// repack all before processing
	for _, pack := range packs {
		total += self.__filter(ts, service, pack)
	}
	for _, pack := range packs {
		if pack.Len() > 0 {
			self.queue <- msg_t{service: service, pack: pack}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunPartial(ts time.Time, service Service, packs ...Repack) (total int, last int) {
	self.mx.Lock()
	part := cap(self.queue) - len(self.queue)
	// repack all before processing
	for i := 0; i < len(packs) && last < part; i++ {
		if added := self.__filter(ts, service, packs[i]); added > 0 {
			total += added
			last++
		}
	}
	part = 0
	for i := 0; part < last; i++ {
		if packs[i].Len() > 0 {
			self.queue <- msg_t{service: service, pack: packs[i]}
			part++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunSimple(service Service, pack Pack) (err error) {
	self.mx.Lock()
	select {
	case self.queue <- msg_t{service: service, pack: pack}:
	default:
		err = fmt.Errorf("OVERFLOW")
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

func (self *Runner_t) Close() {
	close(self.queue)
	self.wg.Wait()
}
