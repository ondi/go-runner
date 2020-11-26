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

func (*MyService_t) ServiceDo(in Pack) {}

type MyPack_t struct {
	id string
}

func (*MyPack_t) Len() int {
	return 1
}

func (self *MyPack_t) IDString(i int) string {
	return self.id
}

func (*MyPack_t) Swap(int, int) {}

func (*MyPack_t) Repack(int) {}

func Test_add01(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 1, 100, 15*time.Second)
	ts := time.Now()
	total, err := r.AddFilter(ts, s, &MyPack_t{id: "1"})
	assert.NilError(t, err)
	assert.Equal(t, total, 1)
	assert.Equal(t, r.SizeFilter(ts), 1)
}

func Test_add02(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()
	total, err := r.AddFilter(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"})
	assert.NilError(t, err)
	assert.Equal(t, total, 2)
	assert.Equal(t, r.SizeFilter(ts), 2)
}

func Test_add03(t *testing.T) {
	s := &MyService_t{}
	r := New(0, 2, 100, 15*time.Second)
	ts := time.Now()
	total, err := r.AddFilter(ts, s, &MyPack_t{id: "1"}, &MyPack_t{id: "2"}, &MyPack_t{id: "3"})
	assert.Equal(t, err.Error(), "OVERFLOW")
	assert.Equal(t, total, 0)
	assert.Equal(t, r.SizeFilter(ts), 0)
}
