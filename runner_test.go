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

func DoneSome(msg Pack, total int) {
	in := msg.(*MyPack_t)
	atomic.AddInt64(&in.done_count, 1)
}

type MyPack_t struct {
	wg         sync.WaitGroup
	In         []string
	running    int64
	do_count   int64
	done_count int64
}

func (self *MyPack_t) Len() int {
	return len(self.In)
}

func (self *MyPack_t) FilterAdd(i int) string {
	return self.In[i]
}

func (self *MyPack_t) FilterDel(i int) string {
	return self.In[i]
}

func (self *MyPack_t) Swap(i int, j int) {
	self.In[i], self.In[j] = self.In[j], self.In[i]
}

func (self *MyPack_t) Running(i int64) int64 {
	self.wg.Add(int(i))
	return atomic.AddInt64(&self.running, i)
}

var my_entry = Entry_t{Module: "default", Function: "default"}

func Test_add01(t *testing.T) {
	r := NewRunner[Pack](10, 1)
	f := NewFilter(100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"test1"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 1)
	assert.Assert(t, in.done_count == 1, in.done_count)
	assert.Assert(t, queued == 1)
	assert.Assert(t, f.Size(ts) == 1)
	assert.Assert(t, r.Size() == 0)
}

func Test_add02(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 2)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 2)
	assert.Assert(t, f.Size(ts) == 2)
	assert.Assert(t, r.Size() == 0)
}

func Test_add03(t *testing.T) {
	r := NewRunner[Pack](10, 3)
	f := NewFilter(100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2", "3"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 3)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 3)
	assert.Assert(t, f.Size(ts) == 3)
	assert.Assert(t, r.Size() == 0)
}

func Test_add04(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, in.do_count == 1)
	assert.Assert(t, in.done_count == 1)
	assert.Assert(t, queued == 1)
	assert.Assert(t, f.Size(ts) == 1)
	assert.Assert(t, r.Size() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	queued = f.Add(ts, my_entry, in, in.Len())
	parts = r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, queued == 2)
	assert.Assert(t, parts > 0)
	assert.Assert(t, f.Size(ts) == 3)
	assert.Assert(t, r.Size() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	queued = f.Add(ts, my_entry, in, in.Len())
	parts = r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == -1)
	assert.Assert(t, queued == 0)
	assert.Assert(t, f.Size(ts) == 3)
	assert.Assert(t, r.Size() == 0)
}

func Test_add05(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 1)
	assert.Assert(t, f.Size(ts) == 1)
	assert.Assert(t, r.Size() == 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	queued = f.Add(ts, my_entry, in, in.Len())
	parts = r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, f.Size(ts) == 3)
	assert.Assert(t, r.Size() == 0)

	removed := f.Del(ts, my_entry, &MyPack_t{In: []string{"3"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, f.Size(ts) == 2)

	removed = f.Del(ts, my_entry, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, f.Size(ts) == 1)

	removed = f.Del(ts, my_entry, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, f.Size(ts) == 0)
}

func Test_add06(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	ts = ts.Add(-10 * time.Second)
	assert.Assert(t, f.Size(ts) == 2)
	assert.Assert(t, r.Size() == 0)
}

func Test_add07(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	ts = ts.Add(10 * time.Second)
	assert.Assert(t, f.Size(ts) == 0)
	assert.Assert(t, r.Size() == 0)
}

func Test_add08(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, f.Size(ts) == 2)
	assert.Assert(t, r.Size() == 0)

	ts = ts.Add(-10 * time.Second)

	removed := f.Del(ts, my_entry, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, f.Size(ts) == 1)
	assert.Assert(t, r.Size() == 0)

	removed = f.Del(ts, my_entry, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 1)
	assert.Assert(t, f.Size(ts) == 0)
	assert.Assert(t, r.Size() == 0)
}

func Test_add09(t *testing.T) {
	r := NewRunner[Pack](10, 2)
	f := NewFilter(100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
	assert.Assert(t, f.Size(ts) == 2)
	assert.Assert(t, r.Size() == 0)

	ts = ts.Add(10 * time.Second)

	removed := f.Del(ts, my_entry, &MyPack_t{In: []string{"1"}})
	assert.Assert(t, removed == 0)
	assert.Assert(t, f.Size(ts) == 0)
	assert.Assert(t, r.Size() == 0)

	removed = f.Del(ts, my_entry, &MyPack_t{In: []string{"2"}})
	assert.Assert(t, removed == 0)
	assert.Assert(t, f.Size(ts) == 0)
	assert.Assert(t, r.Size() == 0)
}

func Test_add10(t *testing.T) {
	r := NewRunner[Pack](10, 3)
	f := NewFilter(100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 2)
}

func Test_add11(t *testing.T) {
	r := NewRunner[Pack](10, 10)
	// f := NewFilter(100, 5*time.Second)
	// ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}}
	queued := in.Len()
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 6)
}

func Test_add12(t *testing.T) {
	r := NewRunner[Pack](10, 10)
	f := NewFilter(0, 0)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}}
	queued := f.Add(ts, my_entry, in, in.Len())
	parts := r.RunAny(my_entry, DoSome, DoneSome, in, queued, queued)
	in.wg.Wait()
	assert.Assert(t, parts == queued)
	assert.Assert(t, queued == 6)
}
