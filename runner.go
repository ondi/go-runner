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
	Running(i int64) int64
}

type Repack interface {
	Pack
	Swap(i int, j int)
}

type Entry_t struct {
	Module   string
	Function string
}

type Filter_t struct {
	Entry Entry_t
	Id    string
}

type Do func(in Pack, begin int, end int)
type Done func(in Pack, total int)

func NoDo(Pack, int, int) {}
func NoDone(Pack, int)    {}

type msg_t struct {
	entry Entry_t
	do    Do
	done  Done
	in    Pack
	begin int
	end   int
	total int
}

type Runner_t struct {
	mx         sync.Mutex
	wg         sync.WaitGroup
	cx         *cache.Cache_t[Filter_t, struct{}]
	wc         *sync.Cond
	qx         chan msg_t
	modules    map[string]int
	functions  map[Entry_t]int
	queue_size int
}

func New(threads int, queue_size int, filter_size int, filter_ttl time.Duration) *Runner_t {
	self := &Runner_t{
		qx:         make(chan msg_t, queue_size),
		modules:    map[string]int{},
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

func (self *Runner_t) FilterAdd(ts time.Time, entry Entry_t, in Repack, length int) (added int) {
	var ok bool
	self.mx.Lock()
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
	self.mx.Unlock()
	return
}

func (self *Runner_t) FilterDel(ts time.Time, entry Entry_t, pack Pack) (removed int) {
	var ok bool
	pack_len := pack.Len()
	self.mx.Lock()
	for i := 0; i < pack_len; i++ {
		if _, ok = self.cx.Remove(ts, Filter_t{Entry: entry, Id: pack.IDString(i)}); ok {
			removed++
		}
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) __queue(entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	if input == 0 || step == 0 {
		return
	}
	if running = input / step; input > running*step {
		running++
	}
	if running > self.queue_size-len(self.qx) {
		running = 0
		return
	}
	in.Running(int64(running))
	parts := step
	for ; parts < input; parts += step {
		self.modules[entry.Module]++
		self.functions[entry]++
		self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: parts - step, end: parts, total: input}
	}
	self.modules[entry.Module]++
	self.functions[entry]++
	self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: parts - step, end: input, total: input}
	return
}

func (self *Runner_t) RunAny(entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	self.mx.Lock()
	running = self.__queue(entry, do, done, in, input, step)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModule(count int, entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	self.mx.Lock()
	if self.modules[entry.Module] < count {
		running = self.__queue(entry, do, done, in, input, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunction(count int, entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		running = self.__queue(entry, do, done, in, input, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModuleWait(count int, entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	self.mx.Lock()
	for {
		if self.modules[entry.Module] < count {
			running = self.__queue(entry, do, done, in, input, step)
			if running > 0 || input == 0 || step == 0 || self.queue_size == 0 {
				break
			}
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunctionWait(count int, entry Entry_t, do Do, done Done, in Pack, input int, step int) (running int) {
	self.mx.Lock()
	for {
		if self.functions[entry] < count {
			running = self.__queue(entry, do, done, in, input, step)
			if running > 0 || input == 0 || step == 0 || self.queue_size == 0 {
				break
			}
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) run() {
	defer self.wg.Done()
	for v := range self.qx {
		v.do(v.in, v.begin, v.end)
		if v.in.Running(-1) == 0 {
			v.done(v.in, v.total)
		}
		self.mx.Lock()
		if temp := self.modules[v.entry.Module]; temp == 1 {
			delete(self.modules, v.entry.Module)
		} else if temp != 0 {
			self.modules[v.entry.Module]--
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

func (self *Runner_t) RangeModule(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.modules {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t) RangeFunction(fn func(key Entry_t, value int) bool) {
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
