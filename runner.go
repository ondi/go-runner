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

type Do[T Pack] func(in T, begin int, end int)
type Done[T Pack] func(in T, total int)

func NoDo[T Pack](T, int, int) {}
func NoDone[T Pack](T, int)    {}

type part_t[T Pack] struct {
	entry Entry_t
	do    Do[T]
	done  Done[T]
	in    T
	begin int
	end   int
	total int
}

type Runner_t[T Pack] struct {
	mx         sync.Mutex
	wg         sync.WaitGroup
	wc         *sync.Cond
	qx         chan part_t[T]
	modules    map[string]int
	functions  map[Entry_t]int
	queue_size int
}

type Entry_t struct {
	Module   string
	Function string
}

func NewRunner[T Pack](threads int, queue_size int) *Runner_t[T] {
	self := &Runner_t[T]{
		qx:         make(chan part_t[T], queue_size),
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

func (self *Runner_t[T]) __queue(entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) int {
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
		self.qx <- part_t[T]{entry: entry, do: do, done: done, in: in, begin: A, end: B, total: length}
	}
	return parts
}

func (self *Runner_t[T]) __increase(entry Entry_t, n int) {
	self.modules[entry.Module] += n
	self.functions[entry] += n
}

func (self *Runner_t[T]) __decrease(entry Entry_t, n int) {
	var temp int
	if temp = self.modules[entry.Module]; temp == n {
		delete(self.modules, entry.Module)
	} else if temp > n {
		self.modules[entry.Module] -= n
	}
	if temp = self.functions[entry]; temp == n {
		delete(self.functions, entry)
	} else if temp > n {
		self.functions[entry] -= n
	}
}

func (self *Runner_t[T]) RunAny(entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) (res int) {
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

func (self *Runner_t[T]) RunModule(count int, entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) (res int) {
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

func (self *Runner_t[T]) RunFunction(count int, entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) (res int) {
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

func (self *Runner_t[T]) RunModuleWait(count int, entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) (res int) {
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

func (self *Runner_t[T]) RunFunctionWait(count int, entry Entry_t, do Do[T], done Done[T], in T, length int, parts int) (res int) {
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

func (self *Runner_t[T]) run() {
	defer self.wg.Done()
	for v := range self.qx {
		v.do(v.in, v.begin, v.end)
		if v.in.Running(-1) == 0 {
			v.done(v.in, v.total)
		}
		self.mx.Lock()
		self.__decrease(v.entry, 1)
		self.wc.Broadcast()
		self.mx.Unlock()
	}
}

func (self *Runner_t[T]) RangeModule(fn func(key string, value int) bool) {
	self.mx.Lock()
	for k, v := range self.modules {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t[T]) RangeFunction(fn func(key Entry_t, value int) bool) {
	self.mx.Lock()
	for k, v := range self.functions {
		if !fn(k, v) {
			self.mx.Unlock()
			return
		}
	}
	self.mx.Unlock()
}

func (self *Runner_t[T]) Size() int {
	return len(self.qx)
}

func (self *Runner_t[T]) Close() {
	self.mx.Lock()
	self.queue_size = 0
	self.mx.Unlock()
	close(self.qx)
	self.wg.Wait()
}
