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
	Name() string
	Do(msg interface{}) error
	Save(msg interface{}) error
	Error(msg interface{}, err error)
}

type Pack interface {
	Len() int
	GetID(i int) string
	Swap(i int, j int)
	Repack(to int) interface{}
}

type Runner interface {
	Add(ts time.Time, srv Service, in Pack) (num int, err error)
	Size() (filter int, queue int)
	Close()
}

type Stat func(service string, diff time.Duration, num int)

func NoStat(string, time.Duration, int) {}

type msg_t struct {
	srv Service
	msg interface{}
	num int
}

type Runner_t struct {
	mx sync.Mutex
	cx *cache.Cache_t
	in chan msg_t
	st Stat
	wg sync.WaitGroup
}

func New(threads int, queue int, limit int, ttl time.Duration, stat Stat) (self *Runner_t) {
	self = &Runner_t{
		cx: cache.New(limit, ttl, cache.Drop),
		in: make(chan msg_t, queue),
		st: stat,
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
			srv.Name()+in.GetID(num),
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
				self.cx.Remove(ts, srv.Name()+in.GetID(i))
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
	for v := range self.in {
		ts = time.Now()
		if err = v.srv.Do(v.msg); err == nil {
			err = v.srv.Save(v.msg)
		}
		if err != nil {
			v.srv.Error(v.msg, err)
		}
		self.st(v.srv.Name(), time.Since(ts), v.num)
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
