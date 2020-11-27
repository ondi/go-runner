//
//
//

package runner

import (
	"fmt"
	"sync"
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
	RunAll(ts time.Time, srv Service, in ...Repack) (total int, err error)
	RunPartial(ts time.Time, srv Service, in ...Repack) (total int, last int)
	RunSimple(srv Service, in Pack) (err error)
	Remove(ts time.Time, srv Name, in PackID) (removed int)
	SizeFilter(ts time.Time) int
	SizeQueue() int
	Close()
}

type msg_t struct {
	srv  Service
	pack Pack
}

type Runner_t struct {
	mx sync.Mutex
	cx *cache.Cache_t
	in chan msg_t
	wg sync.WaitGroup
}

func New(threads int, queue int, limit int, ttl time.Duration) (self *Runner_t) {
	self = &Runner_t{
		cx: cache.New(limit, ttl, cache.Drop),
		in: make(chan msg_t, queue),
	}
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return
}

func (self *Runner_t) __filter(ts time.Time, srv Service, in Repack) (i int) {
	var ok bool
	last := in.Len() - 1
	for i <= last {
		if _, ok = self.cx.Push(
			ts,
			srv.ServiceName()+in.IDString(i),
			func() interface{} { return nil },
			func(interface{}) interface{} { return nil },
		); ok {
			i++
		} else {
			in.Swap(i, last)
			last--
		}
	}
	in.Resize(i)
	return
}

func (self *Runner_t) RunAll(ts time.Time, srv Service, in ...Repack) (total int, err error) {
	self.mx.Lock()
	if len(in) > cap(self.in)-len(self.in) {
		self.mx.Unlock()
		err = fmt.Errorf("OVERFLOW")
		return
	}
	// repack all before processing
	for _, pack := range in {
		total += self.__filter(ts, srv, pack)
	}
	for _, pack := range in {
		if pack.Len() > 0 {
			self.in <- msg_t{srv: srv, pack: pack}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunPartial(ts time.Time, srv Service, in ...Repack) (total int, last int) {
	self.mx.Lock()
	part := cap(self.in) - len(self.in)
	// repack all before processing
	for i := 0; i < len(in) && last < part; i++ {
		if added := self.__filter(ts, srv, in[i]); added > 0 {
			total += added
			last++
		}
	}
	for i := 0; i < last; {
		if in[i].Len() > 0 {
			self.in <- msg_t{srv: srv, pack: in[i]}
			i++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunSimple(srv Service, in Pack) (err error) {
	self.mx.Lock()
	select {
	case self.in <- msg_t{srv: srv, pack: in}:
	default:
		err = fmt.Errorf("OVERFLOW")
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, srv Name, in PackID) (res int) {
	self.mx.Lock()
	var ok bool
	for i := 0; i < in.Len(); i++ {
		if _, ok = self.cx.Remove(ts, srv.ServiceName()+in.IDString(i)); ok {
			res++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.in {
		v.srv.ServiceDo(v.pack)
	}
}

func (self *Runner_t) SizeFilter(ts time.Time) (res int) {
	self.mx.Lock()
	res = self.cx.Size(ts)
	self.mx.Unlock()
	return
}

func (self *Runner_t) SizeQueue() int {
	return len(self.in)
}

func (self *Runner_t) Close() {
	close(self.in)
	self.wg.Wait()
}
