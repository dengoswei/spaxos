package spaxos

import (
	"testing"

	pb "spaxos/spaxospb"
)

func TestFakeNetwork(t *testing.T) {
	printIndicate()

	selfid := uint64(1)
	testid := uint64(2)

	fnet := NewFakeNetwork(selfid)
	assert(nil != fnet)

	sendc := fnet.GetSendChan()
	assert(nil != sendc)
	recvc := fnet.GetRecvChan()
	assert(nil != recvc)

	tsendc := make(chan pb.Message)
	done := make(chan struct{})
	go fnet.run(tsendc, done)
	defer close(done)

	smsg := pb.Message{From: selfid, To: testid}
	sendc <- smsg
	trmsg := <-tsendc
	assert(trmsg.From == selfid)
	assert(trmsg.To == testid)

	rmsg := pb.Message{From: testid, To: selfid}
	fnet.crecvc <- rmsg
	tsmsg := <-recvc
	assert(tsmsg.To == selfid)
	assert(tsmsg.From == testid)
}

func TestFakeNetworkCenter(t *testing.T) {
	printIndicate()

	groups := make(map[uint64]bool)
	groups[uint64(1)] = true
	groups[uint64(2)] = true
	groups[uint64(3)] = true

	fcenter := NewFakeNetworkCenter(groups)
	assert(nil != fcenter)

	for id, _ := range groups {
		fnet := fcenter.Get(id)
		assert(nil != fnet)
		assert(fnet.id == id)
	}

	go fcenter.Run()
	defer fcenter.Stop()

	msg := pb.Message{From: 1, To: 2}
	fnet := fcenter.Get(1)
	fnet.GetSendChan() <- msg

	rmsg := <-fcenter.Get(2).GetRecvChan()
	assert(rmsg.From == 1)
	assert(rmsg.To == 2)

	msg.From = 3
	msg.To = 1
	fcenter.Get(3).GetSendChan() <- msg
	rmsg = <-fcenter.Get(1).GetRecvChan()
	assert(rmsg.From == 3)
	assert(rmsg.To == 1)
}

func TestFakeStorage(t *testing.T) {
	printIndicate()

	store := NewFakeStorage()
	assert(nil != store)
	assert(nil != store.table)

	hs := randHardState()
	assert(0 != hs.Index)

	err := store.Store([]pb.HardState{hs})
	assert(nil == err)

	newhs, err := store.Get(hs.Index)
	assert(nil == err)
	assert(true == hs.AcceptedValue.Equal(newhs.AcceptedValue))
	assert(true == hs.Equal(&newhs))
}
