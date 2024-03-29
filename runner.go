//
//
//

package runner

import (
	"sync"
)

type Pack interface {
	Len() int
	Running(i int64) int64
}

type Entry_t struct {
	Module   string
	Function string
}

type Do func(in Pack, begin int, end int)

func NoDo(Pack, int, int) {}

type msg_t struct {
	entry Entry_t
	do    Do
	done  Do
	in    Pack
	begin int
	end   int
	total int
}

type Runner_t struct {
	mx         sync.Mutex
	wg         sync.WaitGroup
	wc         *sync.Cond
	qx         chan msg_t
	modules    map[string]int
	functions  map[Entry_t]int
	queue_size int
}

func NewRunner(threads int, queue_size int) *Runner_t {
	self := &Runner_t{
		qx:         make(chan msg_t, queue_size),
		modules:    map[string]int{},
		functions:  map[Entry_t]int{},
		queue_size: queue_size,
	}
	self.wc = sync.NewCond(&self.mx)
	for i := 0; i < threads; i++ {
		self.wg.Add(1)
		go self.run()
	}
	return self
}

func (self *Runner_t) __queue(entry Entry_t, do Do, done Do, in Pack, length int, parts int) int {
	if length == 0 || parts == 0 || self.queue_size == 0 {
		return -1
	}
	if parts > length {
		parts = length
	}
	if parts > self.queue_size-len(self.qx) {
		return 0
	}
	step := length / parts
	rest := length - parts*step
	// (length - rest) / step
	in.Running(int64(parts))
	for A, B := 0, step; A < length; A, B = B, B+step {
		self.modules[entry.Module]++
		self.functions[entry]++
		if rest > 0 {
			rest--
			B++
		}
		self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: A, end: B, total: length}
	}
	return parts
}

func (self *Runner_t) RunAny(entry Entry_t, do Do, done Do, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	res = self.__queue(entry, do, done, in, length, parts)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModule(count int, entry Entry_t, do Do, done Do, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	if self.modules[entry.Module] < count {
		res = self.__queue(entry, do, done, in, length, parts)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunction(count int, entry Entry_t, do Do, done Do, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		res = self.__queue(entry, do, done, in, length, parts)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModuleWait(count int, entry Entry_t, do Do, done Do, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	for {
		if self.modules[entry.Module] < count {
			if res = self.__queue(entry, do, done, in, length, parts); res != 0 {
				break
			}
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunctionWait(count int, entry Entry_t, do Do, done Do, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	for {
		if self.functions[entry] < count {
			if res = self.__queue(entry, do, done, in, length, parts); res != 0 {
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
			v.done(v.in, 0, v.total)
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

func (self *Runner_t) Size() int {
	return len(self.qx)
}

func (self *Runner_t) Close() {
	self.mx.Lock()
	self.queue_size = 0
	self.mx.Unlock()
	close(self.qx)
	self.wg.Wait()
}
