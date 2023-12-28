//
//
//

package runner

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"
)

func DoSome(msg Pack, begin int, end int) {
	in := msg.(*MyPack_t)
	atomic.AddInt64(&in.do_count, 1)
}

func DoneSome(msg Pack) {
	in := msg.(*MyPack_t)
	atomic.AddInt64(&in.done_count, 1)
}

type MyPack_t struct {
	wg         sync.WaitGroup
	In         []string
	running    int64
	do_count   int64
	done_count int64
	no_resize  bool
}

func (self *MyPack_t) Len() int {
	return len(self.In)
}

func (self *MyPack_t) IDString(i int) string {
	return self.In[i]
}

func (self *MyPack_t) Swap(i int, j int) {
	if self.no_resize {
		return
	}
	self.In[i], self.In[j] = self.In[j], self.In[i]
}

func (self *MyPack_t) Resize(i int) {
	if self.no_resize {
		return
	}
	self.In = self.In[:i]
}

func (self *MyPack_t) Running(i int64) int64 {
	self.wg.Add(int(i))
	return atomic.AddInt64(&self.running, i)
}

var name = Entry_t{Service: "default", Function: "dosome"}

func Test_add01(t *testing.T) {
	r := New(10, 1, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"test1"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 1)
	assert.Assert(t, in.done_count == 1, in.done_count)
	assert.Assert(t, queued == 1)
	assert.Assert(t, r.SizeFilter(ts) == 1)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add02(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 2)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 2)
	assert.Assert(t, r.SizeFilter(ts) == 2)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add03(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2", "3"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 2)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 2)
	assert.Assert(t, r.SizeFilter(ts) == 2)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add04(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 1)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 1)
	assert.Assert(t, r.SizeFilter(ts) == 1)
	assert.Assert(t, r.SizeQueue() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	_, _, queued, parts = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, queued == 2)
	assert.Assert(t, parts > 0)
	assert.Assert(t, r.SizeFilter(ts) == 3)
	assert.Assert(t, r.SizeQueue() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	_, _, queued, parts = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 0)
	assert.Assert(t, r.SizeFilter(ts) == 3)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add05(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 1)
	assert.Assert(t, r.SizeFilter(ts) == 1)
	assert.Assert(t, r.SizeQueue() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	_, _, queued, parts = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, r.SizeFilter(ts) == 3)
	assert.Assert(t, r.SizeQueue() == 0)

	removed := r.Remove(ts, name, &MyPack_t{In: []string{"3"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, r.SizeFilter(ts) == 2)

	removed = r.Remove(ts, name, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, r.SizeFilter(ts) == 1)

	removed = r.Remove(ts, name, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, r.SizeFilter(ts) == 0)
}

func Test_add06(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	ts = ts.Add(-10 * time.Second)
	assert.Assert(t, r.SizeFilter(ts) == 2)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add07(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	ts = ts.Add(10 * time.Second)
	assert.Assert(t, r.SizeFilter(ts) == 0)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add08(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, r.SizeFilter(ts) == 2)
	assert.Assert(t, r.SizeQueue() == 0)

	ts = ts.Add(-10 * time.Second)

	removed := r.Remove(ts, name, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, r.SizeFilter(ts) == 1)
	assert.Assert(t, r.SizeQueue() == 0)

	removed = r.Remove(ts, name, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, r.SizeFilter(ts) == 0)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add09(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, r.SizeFilter(ts) == 2)
	assert.Assert(t, r.SizeQueue() == 0)

	ts = ts.Add(10 * time.Second)

	removed := r.Remove(ts, name, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 0)
	assert.Assert(t, r.SizeFilter(ts) == 0)
	assert.Assert(t, r.SizeQueue() == 0)

	removed = r.Remove(ts, name, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 0)
	assert.Assert(t, r.SizeFilter(ts) == 0)
	assert.Assert(t, r.SizeQueue() == 0)
}

func Test_add10(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "2"}}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 1)
}

func Test_add11(t *testing.T) {
	r := New(10, 10, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}, no_resize: true}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 6)
}

func Test_add12(t *testing.T) {
	r := New(10, 10, 0, 0)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}, no_resize: true}
	_, _, queued, parts := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 6)
}
