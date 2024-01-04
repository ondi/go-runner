//
//
//

package runner

import (
	"sync"
)

type Repack interface {
	Len() int
	IDString(i int) string
	Swap(i int, j int)
	Running(i int64) int64
}

type Entry_t struct {
	Module   string
	Function string
}

type Do func(in Repack, begin int, end int)
type Done func(in Repack, total int)

func NoDo(Repack, int, int) {}
func NoDone(Repack, int)    {}

type msg_t struct {
	entry Entry_t
	do    Do
	done  Done
	in    Repack
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

func (self *Runner_t) __queue(entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	if input == 0 || step == 0 || self.queue_size == 0 {
		return -1
	}
	if running = input / step; input > running*step {
		running++
	}
	if running > self.queue_size-len(self.qx) {
		return 0
	}
	in.Running(int64(running))
	A, B := 0, step
	for A < input {
		self.modules[entry.Module]++
		self.functions[entry]++
		if B > input {
			B = input
		}
		self.qx <- msg_t{entry: entry, do: do, done: done, in: in, begin: A, end: B, total: input}
		A, B = B, B+step
	}
	return
}

func (self *Runner_t) RunAny(entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	self.mx.Lock()
	running = self.__queue(entry, do, done, in, input, step)
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModule(count int, entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	self.mx.Lock()
	if self.modules[entry.Module] < count {
		running = self.__queue(entry, do, done, in, input, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunction(count int, entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	self.mx.Lock()
	if self.functions[entry] < count {
		running = self.__queue(entry, do, done, in, input, step)
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunModuleWait(count int, entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	self.mx.Lock()
	for {
		if self.modules[entry.Module] < count {
			if running = self.__queue(entry, do, done, in, input, step); running != 0 {
				break
			}
		}
		self.wc.Wait()
	}
	self.mx.Unlock()
	return
}

func (self *Runner_t) RunFunctionWait(count int, entry Entry_t, do Do, done Done, in Repack, input int, step int) (running int) {
	self.mx.Lock()
	for {
		if self.functions[entry] < count {
			if running = self.__queue(entry, do, done, in, input, step); running != 0 {
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
