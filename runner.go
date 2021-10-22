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

type Aggregate interface {
	Total(int)
}

type Call func(agg Aggregate, in interface{})

type Runner interface {
	RunRepack(ts time.Time, name string, fn Call, agg Aggregate, packs []Repack) (queued int, input int, last int)
	Running() int64
	SizeFilter(ts time.Time) int
	SizeQueue() int
	FlushFilter(ts time.Time)
	Close()
}

type msg_t struct {
	name string
	fn   Call
	agg  Aggregate
	pack PackID
	ts   time.Time
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

// repack all before processing
func (self *Runner_t) RunRepack(ts time.Time, name string, fn Call, agg Aggregate, packs []Repack) (queued int, input int, last int) {
	self.mx.Lock()
	any := cap(self.queue) - len(self.queue)
	for any > 0 && last < len(packs) {
		input += packs[last].Len()
		if add := self.__repack(ts, name, packs[last]); add > 0 {
			queued += add
			any--
		}
		last++
	}
	agg.Total(queued)
	for any = 0; any < last; any++ {
		if packs[any].Len() > 0 {
			self.queue <- msg_t{name: name, fn: fn, agg: agg, pack: packs[any], ts: ts}
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) __repack(ts time.Time, name string, pack Repack) (i int) {
	var ok bool
	last := pack.Len() - 1
	for i <= last {
		if _, ok = self.cx.Create(
			ts,
			name+pack.IDString(i),
			func() interface{} { return nil },
			func(prev interface{}) interface{} { return prev },
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

func (self *Runner_t) remove(ts time.Time, name string, pack PackID) (removed int) {
	var ok bool
	self.mx.Lock()
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
		v.fn(v.agg, v.pack)
		self.remove(v.ts, v.name, v.pack)
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
