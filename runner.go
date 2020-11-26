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

type PackFilter interface {
	PackID
	Swap(i int, j int)
	Repack(to int) PackFilter
}

type Name interface {
	ServiceName() string
}

type Service interface {
	Name
	ServiceDo(msg Pack)
}

type Runner interface {
	AddFilter(ts time.Time, srv Service, in PackFilter) (num int, err error)
	DelFilter(ts time.Time, srv Name, in PackID) (num int)
	AddPack(srv Service, in Pack) (err error)
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

func (self *Runner_t) AddFilter(ts time.Time, srv Service, in PackFilter) (i int, err error) {
	var ok bool
	last := in.Len() - 1
	self.mx.Lock()
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
	if i > 0 {
		if err = self.AddPack(srv, in.Repack(i)); err != nil {
			for last = 0; last < i; last++ {
				self.cx.Remove(ts, srv.ServiceName()+in.IDString(last))
			}
			i = 0
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) DelFilter(ts time.Time, srv Name, in PackID) (res int) {
	var ok bool
	self.mx.Lock()
	for i := 0; i < in.Len(); i++ {
		if _, ok = self.cx.Remove(
			ts,
			srv.ServiceName()+in.IDString(i),
		); ok {
			res++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) AddPack(srv Service, in Pack) error {
	select {
	case self.in <- msg_t{srv: srv, pack: in}:
	default:
		return fmt.Errorf("OVERFLOW")
	}
	return nil
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
