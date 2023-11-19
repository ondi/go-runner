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
	_ = msg.(*MyPack_t)
}

func DoneSome(msg Pack) {
	in := msg.(*MyPack_t)
	in.wg.Wait()
}

type MyPack_t struct {
	wg      sync.WaitGroup
	In      []string
	running int
}

func (self *MyPack_t) Len() int {
	return len(self.In)
}

func (self *MyPack_t) IDString(i int) string {
	return self.In[i]
}

func (self *MyPack_t) Swap(i int, j int) {
	self.In[i], self.In[j] = self.In[j], self.In[i]
}

func (self *MyPack_t) Resize(i int) {
	self.In = self.In[:i]
}

func (self *MyPack_t) Running(i int) int {
	self.wg.Add(i)
	self.running += i
	return self.running
}

var name = Entry_t{Service: "default", Function: "dosome"}

func Test_add01(t *testing.T) {
	r := New(0, 1, 100, 15*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1"}}, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)
}

func Test_add02(t *testing.T) {
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2"}}, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add03(t *testing.T) {
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2", "3"}}, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add04(t *testing.T) {
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1"}}, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)

	_, _, queued = r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"2", "3"}}, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	_, _, queued = r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"2", "3"}}, 1)
	assert.Equal(t, queued, 0)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add05(t *testing.T) {
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1"}}, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)

	_, _, queued = r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"2", "3"}}, 1)
	assert.Equal(t, queued, 1)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	removed := r.Remove(ts, "default", &MyPack_t{In: []string{"3"}})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 2)

	removed = r.Remove(ts, "default", &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)

	removed = r.Remove(ts, "default", &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
}

func Test_add06(t *testing.T) {
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2"}}, 1)
	assert.Equal(t, queued, 2)
	ts = ts.Add(-10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add07(t *testing.T) {
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2"}}, 1)
	assert.Equal(t, queued, 2)
	ts = ts.Add(10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add08(t *testing.T) {
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2"}}, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	ts = ts.Add(-10 * time.Second)

	removed := r.Remove(ts, "default", &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 2)

	removed = r.Remove(ts, "default", &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add09(t *testing.T) {
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "2"}}, 1)
	assert.Equal(t, queued, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	ts = ts.Add(10 * time.Second)

	removed := r.Remove(ts, "default", &MyPack_t{In: []string{"1"}})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)

	removed = r.Remove(ts, "default", &MyPack_t{In: []string{"2"}})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add10(t *testing.T) {
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	_, _, queued := r.RunAny(ts, name, DoSome, DoneSome, &MyPack_t{In: []string{"1", "1", "2"}}, 1)
	assert.Equal(t, queued, 1)
}
