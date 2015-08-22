package spaxos

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	pb "spaxos/spaxospb"
)

func TestFakeSwitch(t *testing.T) {
	printIndicate()

	selfid := uint64(1)
	testid := uint64(2)

	fswitch := NewFakeSwitch(selfid)
	assert(nil != fswitch)

	sendc := fswitch.GetSendChan()
	assert(nil != sendc)
	recvc := fswitch.GetRecvChan()
	assert(nil != recvc)

	tsendc := make(chan pb.Message)
	done := make(chan struct{})
	go fswitch.run(tsendc, done)
	defer close(done)

	smsg := pb.Message{From: selfid, To: testid}
	sendc <- smsg
	trmsg := <-tsendc
	assert(trmsg.From == selfid)
	assert(trmsg.To == testid)

	rmsg := pb.Message{From: testid, To: selfid}
	fswitch.crecvc <- rmsg
	tsmsg := <-recvc
	assert(tsmsg.To == selfid)
	assert(tsmsg.From == testid)
}

func TestFakeSwitchCenter(t *testing.T) {
	printIndicate()

	groups := make(map[uint64]bool)
	groups[uint64(1)] = true
	groups[uint64(2)] = true
	groups[uint64(3)] = true

	fcenter := NewFakeSwitchCenter(groups)
	assert(nil != fcenter)

	for id, _ := range groups {
		fswitch := fcenter.Get(id)
		assert(nil != fswitch)
		assert(fswitch.id == id)
	}

	go fcenter.Run()
	defer fcenter.Stop()

	msg := pb.Message{From: 1, To: 2}
	fswitch := fcenter.Get(1)
	fswitch.GetSendChan() <- msg

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
	assert(true == hs.Equal(&newhs))

	newhs, err = store.Get(hs.Index + 1)
	assert(nil != err)
	assert(err == IndexNotExist)
	assert(0 == newhs.Index)
}

func TestGroupEntry(t *testing.T) {
	printIndicate()

	entry := &GroupEntry{Id: 1, Ip: "127.0.0.1", Port: 10001}
	s, err := json.Marshal(entry)
	assert(nil == err)
	os.Stdout.Write(s)
	println()

	conststr := `{"id": 1, "ip": "127.0.0.1", "port": 10001}`
	newentry := &GroupEntry{}
	err = json.Unmarshal([]byte(conststr), newentry)
	assert(nil == err)
	fmt.Printf("%v\n", newentry)
}

func TestReadConfig(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	fmt.Printf("%d %v\n", len(c.Groups), c)

	assert(0 < c.Selfid)
	groups := c.GetGroupIds()
	assert(len(groups) == len(c.Groups))
	entry := c.GetEntry(c.Selfid)
	assert(c.Selfid == entry.Id)
}
