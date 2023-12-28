//
//
//

package runner

import (
	"sync"
	"time"

	cache "github.com/ondi/go-ttl-cache"
)

type Pack interface {
	Len() int
	IDString(i int) string
}

type Repack interface {
	Pack
	Swap(i int, j int)
	Resize(i int)
	Running(i int64) int64
}

type Entry_t struct {
	Service  string
	Function string
}

type Filter_t struct {
	Entry Entry_t
	Id    string
}

type Do func(in Pack, begin int, end int)
type Done func(in Pack)

func NoDo(Pack, int, int) {}

func NoDone(Pack) {}

type msg_t struct {
	entry Entry_t
	do    Do
	done  Done
	in    Repack
	begin int
	end   int
}

type Runner_t struct {
	mx         sync.Mutex
	wg         sync.WaitGroup
	cx         *cache.Cache_t[Filter_t, struct{}]
	wc         *sync.Cond
	qx         chan msg_t
	services   map[string]int
	functions  map[Entry_t]int
	queue_size int
}

func New(threads int, queue_size int, filter_size int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		qx:         make(chan msg_t, queue_size),
		services:   map[string]int{},
		functions:  map[Entry_t]int{},
		queue_size: queue_size,
	}
	self.cx = cache.New(filter_size, filter_ttl, cache.Drop[Filter_t, struct{}])
	self.wc = sync.NewCond(&self.mx)
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __repack(ts time.Time, entry Entry_t, in Repack, length int) (added int) {
	var ok bool
	for added < length {
		_, ok = self.cx.Create(
			ts,
			Filter_t{Entry: entry, Id: in.IDString(added)},
			func(*struct{}) {},
			func(*struct{}) {},
		)
		if ok {
			added++
		} else {
			length--
			in.Swap(added, length)
		}
	}
	in.Resize(added)
	return
}

func (self *Runner_t) __queue(ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	if input = in.Len(); input == 0 || step == 0 {
		return
	}
	if running = input / step; input > running*step {
		running++
	}
	temp := self.queue_size - len(self.qx)
	if running > temp {
		running = temp
		queued = temp * step
	} else {
		queued = input
	}
	// filter Repack and ask it to fit into available space, Repack.Swap() and Repack.Resize() may ignore it.
	self.__repack(ts, entry, in, queued)
	if temp = in.Len(); temp > queued || temp == 0 {
		return 0, input, 0
	}
	queued = temp
	if running = queued / step; queued > running*step {
		running++
	}
	in.Running(int64(running))
	for temp = step; temp < queued; temp += step {
		self.services[entry.Service]++
		self.functions[entry]++
		self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: temp - step, end: temp}
	}
	self.services[entry.Service]++
	self.functions[entry]++
	self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: temp - step, end: queued}
	return
}

func (self *Runner_t) RunAny(ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	self.mx.Lock()
	running, input, queued = self.__queue(ts, entry, do, done, in, step)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunService(count int, ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	self.mx.Lock()
	if self.services[entry.Service] < count {
		running, input, queued = self.__queue(ts, entry, do, done, in, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunEntry(count int, ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		running, input, queued = self.__queue(ts, entry, do, done, in, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunServiceWait(count int, ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	self.mx.Lock()
	for self.services[entry.Service] >= count {
		self.wc.Wait()
	}
	running, input, queued = self.__queue(ts, entry, do, done, in, step)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunEntryWait(count int, ts time.Time, entry Entry_t, do Do, done Done, in Repack, step int) (running int, input int, queued int) {
	self.mx.Lock()
	for self.functions[entry] >= count {
		self.wc.Wait()
	}
	running, input, queued = self.__queue(ts, entry, do, done, in, step)
	self.mx.Unlock()
	return
}

func (self *Runner_t) Remove(ts time.Time, entry Entry_t, pack Pack) (removed int) {
	self.mx.Lock()
	pack_len := pack.Len()
	for i := 0; i < pack_len; i++ {
		if _, ok := self.cx.Remove(ts, Filter_t{Entry: entry, Id: pack.IDString(i)}); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.qx {
		v.do(v.in, v.begin, v.end)
		if v.in.Running(-1) == 0 {
			v.done(v.in)
		}
		self.mx.Lock()
		if temp := self.services[v.entry.Service]; temp == 1 {
			delete(self.services, v.entry.Service)
		} else if temp != 0 {
			self.services[v.entry.Service]--
		}
		if temp := self.functions[v.entry]; temp == 1 {
			delete(self.functions, v.entry)
		} else if temp != 0 {
			self.functions[v.entry]--
		}
		self.wc.Broadcast()
		self.mx.Unlock()
	}
}

func (self *Runner_t) RangeSv(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.services {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFn(fn func(key Entry_t, value int) bool) {
	self.mx.Lock()
	for k, v := range self.functions {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFilter(ts time.Time, fn func(key Filter_t, value struct{}) bool) {
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
	return len(self.qx)
}

func (self *Runner_t) Close() {
	self.mx.Lock()
	self.queue_size = 0
	self.mx.Unlock()
	close(self.qx)
	self.wg.Wait()
}

func ThinOut(in_len, out_len int) (out []int) {
	part_size := in_len / out_len
	rest := in_len - out_len*part_size
	for i := 0; i < in_len; i += part_size {
		out = append(out, (i+i+part_size)/2)
		if rest > 0 {
			i++
			rest--
		}
	}
	return
}
