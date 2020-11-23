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

type Pack interface {
	Len() int
	Swap(i int, j int)
	Repack(to int) interface{}
	IDString(i int) string
}

type Runner interface {
	Add(ts time.Time, srv Service, in Pack) (num int, err error)
	Size() (filter int, queue int)
	Close()
}

type msg_t struct {
	srv Service
	msg interface{}
	num int
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
		select {
		case self.in <- msg_t{srv: srv, msg: in.Repack(num), num: num}:
		default:
			for i := 0; i < num; i++ {
				self.cx.Remove(ts, srv.ServiceName()+in.IDString(i))
			}
			num, err = 0, fmt.Errorf("OVERFLOW")
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	var err error
	var ts time.Time
	for msg := range self.in {
		ts = time.Now()
		if err = msg.srv.ServiceDo(msg.msg); err != nil {
			msg.srv.ServiceError(msg.msg, err)
		} else if err = msg.srv.ServiceSave(msg.msg); err != nil {
			msg.srv.ServiceError(msg.msg, err)
		}
		self.st(msg.srv.ServiceName(), time.Since(ts), msg.num)
	}
}

func (self *Runner_t) Size() (filter int, queue int) {
	self.mx.Lock()
	filter = self.cx.Size()
	queue = len(self.in)
	self.mx.Unlock()
	return
}

func (self *Runner_t) Close() {
	close(self.in)
	self.wg.Wait()
}
