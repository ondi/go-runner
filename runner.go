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

type Service interface {
	ServiceName() string
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
	AddSimple(srv Service, in Simple) (num int, err error)
	SizeF(ts time.Time) int
	SizeQ() int
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

func (self *Runner_t) Add(ts time.Time, srv Service, in Pack) (num int, err error) {
	self.mx.Lock()
	var ok bool
	last := in.Len() - 1
	for num <= last {
		if _, ok = self.cx.Push(
			ts,
			srv.ServiceName()+in.IDString(num),
			func() interface{} { return nil },
			func(interface{}) interface{} { return nil },
		); ok {
			num++
		} else {
			in.Swap(num, last)
			last--
		}
	}
	if num > 0 {
		if num, err = self.AddSimple(srv, in.Repack(num)); err != nil {
			for i := 0; i < num; i++ {
				self.cx.Remove(ts, srv.ServiceName()+in.IDString(i))
			}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) AddSimple(srv Service, in Simple) (num int, err error) {
	select {
	case self.in <- msg_t{srv: srv, pack: in}:
	default:
		num, err = 0, fmt.Errorf("OVERFLOW")
	}
	return
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

func (self *Runner_t) SizeF(ts time.Time) (filter int) {
	self.mx.Lock()
	filter = self.cx.Size(ts)
	self.mx.Unlock()
	return
}

func (self *Runner_t) SizeQ(ts time.Time) (queue int) {
	return len(self.in)
}

func (self *Runner_t) Close() {
	close(self.in)
	self.wg.Wait()
}
