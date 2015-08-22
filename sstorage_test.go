package spaxos

import (
	"testing"

	pb "spaxos/spaxospb"
)

func TestNewStorage(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)
	c.Path = "./test_data"

	store, err := NewStorage(c)
	assert(nil == err)
	assert(nil != store)
	defer store.Close()
}

func TestSetAndGetIndex(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)
	c.Path = "./test_data"

	store, err := NewStorage(c)
	assert(nil == err)
	assert(nil != store)
	defer store.Close()

	err = store.SetIndex(1, 10)
	assert(nil == err)

	minIndex, maxIndex, err := store.GetIndex()
	assert(nil == err)
	assert(uint64(1) == minIndex)
	assert(uint64(10) == maxIndex)
}

func TestSetAndGet(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)
	c.Path = "./test_data"

	store, err := NewStorage(c)
	assert(nil == err)
	assert(nil != store)
	defer store.Close()

	hs := randHardState()
	err = store.Set([]pb.HardState{hs})
	assert(nil == err)

	newhs, err := store.Get(hs.Index)
	assert(nil == err)
	assert(true == hs.Equal(&newhs))
}
