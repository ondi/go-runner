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

type Name interface {
	ServiceName() string
}

type Service interface {
	Name
	ServiceDo(msg interface{}) error
	ServiceSave(msg interface{}) error
	ServiceError(msg interface{}, err error)
}

type Simple interface {
	Len() int
}

type Pack interface {
	Simple
	Swap(i int, j int)
	Repack(to int) Pack
	IDString(i int) string
}

type Runner interface {
	Add(ts time.Time, srv Service, in Pack) (num int, err error)
	Del(ts time.Time, srv Name, in Pack) (num int)
	AddSimple(srv Service, in Simple) (err error)
	SizeFilter(ts time.Time) int
	Size() int
	Close()
}

type msg_t struct {
	srv  Service
	pack Simple
}

type StatFn func(service string, diff time.Duration, num int)

type Runner_t struct {
	mx sync.Mutex
	cx *cache.Cache_t
	in chan msg_t
	st StatFn
	wg sync.WaitGroup
}

type Options func(self *Runner_t)

func Stat(st StatFn) Options {
	return func(self *Runner_t) {
		self.st = st
	}
}

func New(threads int, queue int, limit int, ttl time.Duration, opt ...Options) (self *Runner_t) {
	self = &Runner_t{
		cx: cache.New(limit, ttl, cache.Drop),
		in: make(chan msg_t, queue),
		st: func(string, time.Duration, int) {},
	}
	for _, v := range opt {
		v(self)
	}
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return
}

func (self *Runner_t) Add(ts time.Time, srv Service, in Pack) (i int, err error) {
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
		if err = self.AddSimple(srv, in.Repack(i)); err != nil {
			for last = 0; last < i; last++ {
				self.cx.Remove(ts, srv.ServiceName()+in.IDString(last))
			}
			i = 0
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) Del(ts time.Time, srv Service, in Pack) (num int) {
	var ok bool
	self.mx.Lock()
	for i := 0; i < in.Len(); i++ {
		if _, ok = self.cx.Remove(
			ts,
			srv.ServiceName()+in.IDString(i),
		); ok {
			num++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) AddSimple(srv Service, in Simple) error {
	select {
	case self.in <- msg_t{srv: srv, pack: in}:
	default:
		return fmt.Errorf("OVERFLOW")
	}
	return nil
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	var err error
	var ts time.Time
	for msg := range self.in {
		ts = time.Now()
		if err = msg.srv.ServiceDo(msg.pack); err != nil {
			msg.srv.ServiceError(msg.pack, err)
		} else if err = msg.srv.ServiceSave(msg.pack); err != nil {
			msg.srv.ServiceError(msg.pack, err)
		}
		self.st(msg.srv.ServiceName(), time.Since(ts), msg.pack.Len())
	}
}

func (self *Runner_t) SizeFilter(ts time.Time) (filter int) {
	self.mx.Lock()
	filter = self.cx.Size(ts)
	self.mx.Unlock()
	return
}

func (self *Runner_t) Size() (queue int) {
	return len(self.in)
}

func (self *Runner_t) Close() {
	close(self.in)
	self.wg.Wait()
}
