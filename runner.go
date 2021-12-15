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
	queued  map[string]int
	running int64
	wg      sync.WaitGroup
}

func New(threads int, queue int, filter_limit int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		cx:     cache.New(filter_limit, filter_ttl, cache.Drop),
		queue:  make(chan msg_t, queue),
		queued: map[string]int{},
	}
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, name string, pack Repack) (added int) {
	var ok bool
	last := pack.Len()
	for added < last {
		if _, ok = self.cx.Create(
			ts,
			name+pack.IDString(added),
			func() interface{} { return nil },
			func(prev interface{}) interface{} { return prev },
		); ok {
			added++
		} else {
			last--
			pack.Swap(added, last)
		}
	}
	pack.Resize(added)
	return
}

// Total() should be called before processing
func (self *Runner_t) __queue(ts time.Time, name string, fn Call, agg Aggregate, packs []Repack) (input int, queued int) {
	var last, added int
	available := cap(self.queue) - len(self.queue)
	for available > 0 && last < len(packs) {
		input += packs[last].Len()
		if added = self.__repack(ts, name, packs[last]); added > 0 {
			self.queued[name]++
			queued += added
			available--
		}
		last++
	}
	agg.Total(queued)
	for available = 0; available < last; available++ {
		if packs[available].Len() > 0 {
			self.queue <- msg_t{name: name, fn: fn, agg: agg, pack: packs[available], ts: ts}
		}
	}
	return
}

func (self *Runner_t) RunAny(ts time.Time, name string, fn Call, agg Aggregate, packs []Repack) (input int, queued int) {
	self.mx.Lock()
	input, queued = self.__queue(ts, name, fn, agg, packs)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunExclusive(ts time.Time, name string, fn Call, agg Aggregate, packs []Repack) (input int, queued int) {
	self.mx.Lock()
	if _, ok := self.queued[name]; ok {
		self.mx.Unlock()
		return
	}
	input, queued = self.__queue(ts, name, fn, agg, packs)
	self.mx.Unlock()
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
	if temp, ok := self.queued[name]; temp == 1 {
		delete(self.queued, name)
	} else if ok {
		self.queued[name]--
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

func (self *Runner_t) Queued(name string) (res int) {
	self.mx.Lock()
	res = self.queued[name]
	self.mx.Unlock()
	return
}

func (self *Runner_t) RangeQueued(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.queued {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFilter(ts time.Time, fn func(key interface{}, value interface{}) bool) {
	self.mx.Lock()
	self.cx.Range(ts, fn)
	self.mx.Unlock()
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
