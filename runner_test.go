//
//
//

package runner

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"
)

func DoSome(msg Pack, begin int, end int) {
	in := msg.(*MyPack_t)
	in.mx.Lock()
	in.do_count++
	in.mx.Unlock()
}

func DoneSome(msg Pack) {
	in := msg.(*MyPack_t)
	in.mx.Lock()
	in.done_count++
	in.mx.Unlock()
}

type MyPack_t struct {
	mx         sync.Mutex
	wg         sync.WaitGroup
	In         []string
	running    int
	do_count   int
	done_count int
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

func (self *MyPack_t) Running(i int) (res int) {
	self.wg.Add(i)
	self.mx.Lock()
	self.running += i
	res = self.running
	self.mx.Unlock()
	return
}

var name = Entry_t{Service: "default", Function: "dosome"}

func Test_add01(t *testing.T) {
	r := New(10, 1, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"test1"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, in.do_count, 1)
	assert.Equal(t, in.done_count, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 0)

}

func Test_add02(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, in.do_count, 2)
	assert.Equal(t, in.done_count, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add03(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2", "3"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, in.do_count, 2)
	assert.Equal(t, in.done_count, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add04(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, in.do_count, 1)
	assert.Equal(t, in.done_count, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	_, _, queued = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 3)
	assert.Equal(t, r.SizeQueue(), 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	parts, _, queued = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 0)
	assert.Equal(t, r.SizeFilter(ts), 3)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add05(t *testing.T) {
	r := New(10, 2, 100, 15*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 0)

	in = &MyPack_t{In: []string{"2", "3"}}
	parts, _, queued = r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 3)
	assert.Equal(t, r.SizeQueue(), 0)

	removed := r.Remove(ts, name.Service, &MyPack_t{In: []string{"3"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 2)

	removed = r.Remove(ts, name.Service, &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)

	removed = r.Remove(ts, name.Service, &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
}

func Test_add06(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 2)
	ts = ts.Add(-10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add07(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 2)
	ts = ts.Add(10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add08(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 0)

	ts = ts.Add(-10 * time.Second)

	removed := r.Remove(ts, name.Service, &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 0)

	removed = r.Remove(ts, name.Service, &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add09(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 0)

	ts = ts.Add(10 * time.Second)

	removed := r.Remove(ts, name.Service, &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 0)

	removed = r.Remove(ts, name.Service, &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 0)
}

func Test_add10(t *testing.T) {
	r := New(10, 2, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "2"}}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 1)
}

func Test_add11(t *testing.T) {
	r := New(10, 10, 100, 5*time.Second)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}, no_resize: true}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 6)
}

func Test_add12(t *testing.T) {
	r := New(10, 10, 0, 0)
	ts := time.Now()

	in := &MyPack_t{In: []string{"1", "1", "1", "2", "2", "2"}, no_resize: true}
	parts, _, queued := r.RunAny(ts, name, DoSome, DoneSome, in, 1)
	in.wg.Wait()
	assert.Equal(t, parts, queued)
	assert.Equal(t, queued, 6)
}
