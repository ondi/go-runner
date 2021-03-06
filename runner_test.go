//
//
//

package runner

import (
	"testing"
	"time"

	"gotest.tools/assert"
)

type MyService_t struct{}

func (*MyService_t) ServiceName() string {
	return "default"
}

func (*MyService_t) ServiceDo(in interface{}) {}

type MyPack_t struct {
	id     string
	resize bool
}

func (self *MyPack_t) Len() int {
	if self.resize {
		return 0
	}
	return 1
}

func (self *MyPack_t) IDString(i int) string {
	return self.id
}

func (*MyPack_t) Swap(int, int) {}

func (self *MyPack_t) Resize(i int) {
	if i == 0 {
		self.resize = true
	}
}

func Test_add01(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 1, 100, 15*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, total, 1)
	assert.Equal(t, last, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)
}

func Test_add02(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add03(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"}, &MyPack_t{id: "3"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add04(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, total, 1)
	assert.Equal(t, last, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)

	total, last = r.RunRepack(ts, s, &MyPack_t{id: "2"}, &MyPack_t{id: "3"})
	assert.Equal(t, total, 1)
	assert.Equal(t, last, 1)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	total, last = r.RunRepack(ts, s, &MyPack_t{id: "2"}, &MyPack_t{id: "3"})
	assert.Equal(t, total, 0)
	assert.Equal(t, last, 0)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add05(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, total, 1)
	assert.Equal(t, last, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 1)

	total, last = r.RunRepack(ts, s, &MyPack_t{id: "2"}, &MyPack_t{id: "3"})
	assert.Equal(t, total, 1)
	assert.Equal(t, last, 1)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	removed := r.Remove(ts, s, &MyPack_t{id: "3"})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 2)

	removed = r.Remove(ts, s, &MyPack_t{id: "2"})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)

	removed = r.Remove(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
}

func Test_add06(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	ts = ts.Add(-10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add07(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	ts = ts.Add(10 * time.Second)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add08(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	ts = ts.Add(-10 * time.Second)

	removed := r.Remove(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
	assert.Equal(t, r.SizeQueue(), 2)

	removed = r.Remove(ts, s, &MyPack_t{id: "2"})
	assert.Equal(t, removed, 1)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add09(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
	assert.Equal(t, r.SizeQueue(), 2)

	ts = ts.Add(10 * time.Second)

	removed := r.Remove(ts, s, &MyPack_t{id: "1"})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)

	removed = r.Remove(ts, s, &MyPack_t{id: "2"})
	assert.Equal(t, removed, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
	assert.Equal(t, r.SizeQueue(), 2)
}

func Test_add10(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 5*time.Second)
	ts := time.Now()

	total, last := r.RunRepack(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.Equal(t, total, 2)
	assert.Equal(t, last, 3)
}
