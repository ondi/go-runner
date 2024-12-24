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

type Do func(in Pack, begin int, end int)
type Done func(in Pack, total int)

func NoDo(Pack, int, int) {}
func NoDone(Pack, int)    {}

type part_t struct {
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
	wc         *sync.Cond
	qx         chan part_t
	modules    map[string]int
	functions  map[Entry_t]int
	queue_size int
}

type Entry_t struct {
	Module   string
	Function string
}

func NewRunner(threads int, queue_size int) *Runner_t {
	self := &Runner_t{
		qx:         make(chan part_t, queue_size),
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

func (self *Runner_t) __queue(entry Entry_t, do Do, done Done, in Pack, length int, parts int) int {
	if parts > length {
		parts = length
	}
	if parts > self.queue_size-len(self.qx) {
		parts = self.queue_size - len(self.qx)
	}
	if parts <= 0 {
		return -1
	}
	in.Running(int64(parts))
	step := length / parts
	rest := length - parts*step
	for A, B := 0, step; A < length; A, B = B, B+step {
		if rest > 0 {
			rest--
			B++
		}
		self.qx <- part_t{entry: entry, do: do, done: done, in: in, begin: A, end: B, total: length}
	}
	return parts
}

func (self *Runner_t) __increase(entry Entry_t, n int) {
	self.modules[entry.Module] += n
	self.functions[entry] += n
}

func (self *Runner_t) __decrease(entry Entry_t, n int) {
	var temp int
	if temp = self.modules[entry.Module]; temp <= n {
		delete(self.modules, entry.Module)
	} else {
		self.modules[entry.Module] -= n
	}
	if temp = self.functions[entry]; temp <= n {
		delete(self.functions, entry)
	} else {
		self.functions[entry] -= n
	}
	self.wc.Broadcast()
}

func (self *Runner_t) RunAny(entry Entry_t, do Do, done Done, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	if res = self.__queue(entry, do, done, in, length, parts); res == -1 {
		self.__increase(entry, 1)
	} else {
		self.__increase(entry, res)
	}
	self.mx.Unlock()
	if res == -1 {
		done(in, 0)
		self.mx.Lock()
		self.__decrease(entry, 1)
		self.mx.Unlock()
	}
	return
}

func (self *Runner_t) RunModule(count int, entry Entry_t, do Do, done Done, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	if self.modules[entry.Module] < count {
		if res = self.__queue(entry, do, done, in, length, parts); res == -1 {
			self.__increase(entry, 1)
		} else {
			self.__increase(entry, res)
		}
	}
	self.mx.Unlock()
	if res == -1 {
		done(in, 0)
		self.mx.Lock()
		self.__decrease(entry, 1)
		self.mx.Unlock()
	}
	return
}

func (self *Runner_t) RunFunction(count int, entry Entry_t, do Do, done Done, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		if res = self.__queue(entry, do, done, in, length, parts); res == -1 {
			self.__increase(entry, 1)
		} else {
			self.__increase(entry, res)
		}
	}
	self.mx.Unlock()
	if res == -1 {
		done(in, 0)
		self.mx.Lock()
		self.__decrease(entry, 1)
		self.mx.Unlock()
	}
	return
}

func (self *Runner_t) RunModuleWait(count int, entry Entry_t, do Do, done Done, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	for {
		if self.modules[entry.Module] < count {
			if res = self.__queue(entry, do, done, in, length, parts); res == -1 {
				self.__increase(entry, 1)
			} else {
				self.__increase(entry, res)
			}
			break
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	if res == -1 {
		done(in, 0)
		self.mx.Lock()
		self.__decrease(entry, 1)
		self.mx.Unlock()
	}
	return
}

func (self *Runner_t) RunFunctionWait(count int, entry Entry_t, do Do, done Done, in Pack, length int, parts int) (res int) {
	self.mx.Lock()
	for {
		if self.functions[entry] < count {
			if res = self.__queue(entry, do, done, in, length, parts); res == -1 {
				self.__increase(entry, 1)
			} else {
				self.__increase(entry, res)
			}
			break
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	if res == -1 {
		done(in, 0)
		self.mx.Lock()
		self.__decrease(entry, 1)
		self.mx.Unlock()
	}
	return
}

func (self *Runner_t) run() {
	for v := range self.qx {
		v.do(v.in, v.begin, v.end)
		if v.in.Running(-1) == 0 {
			v.done(v.in, v.total)
		}
		self.mx.Lock()
		self.__decrease(v.entry, 1)
		self.mx.Unlock()
	}
	self.wg.Done()
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
