package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type storePackage struct {
	outMsgs       []pb.Message
	outHardStates []pb.HardState
}

type Storager interface {
	// store hard state
	Store([]pb.HardState) error

	Get(index uint64) (pb.HardState, error)
}

// TODO: fix interface func & name!!!
type Networker interface {
	GetSendChan() chan pb.Message
	GetRecvChan() chan pb.Message
}

type FakeStorage struct {
	table map[uint64]pb.HardState
}

func NewFakeStorage() *FakeStorage {
	store := &FakeStorage{table: make(map[uint64]pb.HardState)}
	return store
}

func (store *FakeStorage) Store(hss []pb.HardState) error {
	for _, hs := range hss {
		assert(0 < hs.Index)
		store.table[hs.Index] = hs
	}
	return nil
}

func (store *FakeStorage) Get(index uint64) (pb.HardState, error) {
	assert(0 < index)
	if hs, ok := store.table[index]; ok {
		assert(index == hs.Index)
		return hs, nil
	}
	return pb.HardState{}, errors.New("Not Exist")
}

type FakeNetwork struct {
	id uint64
	// attach to fron-end
	fsendc chan pb.Message
	frecvc chan pb.Message

	// attach to real communication channel
	crecvc      chan pb.Message
	sendingMsgs []pb.Message
	recvingMsgs []pb.Message
}

func NewFakeNetwork(id uint64) *FakeNetwork {
	fnet := &FakeNetwork{
		id:     id,
		fsendc: make(chan pb.Message),
		frecvc: make(chan pb.Message),
		crecvc: make(chan pb.Message)}
	return fnet
}

func (fnet *FakeNetwork) GetSendChan() chan pb.Message {
	return fnet.fsendc
}

func (fnet *FakeNetwork) GetRecvChan() chan pb.Message {
	return fnet.frecvc
}

func (fnet *FakeNetwork) run(sendc chan pb.Message) {
	for {
		dsendc, smsg := getMsg(sendc, fnet.sendingMsgs)
		drecvc, rmsg := getMsg(fnet.frecvc, fnet.recvingMsgs)

		select {
		// send msg in sendingMsgsQueue to sendc
		case dsendc <- smsg:
			fnet.sendingMsgs = fnet.sendingMsgs[1:]

		// recv msg from recv append into recvingMsgsQueue
		case msg := <-fnet.crecvc:
			assert(fnet.id == msg.To)
			fnet.recvingMsgs = append(fnet.recvingMsgs, msg)

		// send msg in recvingMsgsQueue to fnet.frecvc
		case drecvc <- rmsg:
			fnet.recvingMsgs = fnet.recvingMsgs[1:]

			// recv msg from fnet.fsendc, apppend into sendingMsgsQueue
		case msg := <-fnet.fsendc:
			fnet.sendingMsgs = append(fnet.sendingMsgs, msg)
		}
	}
}

type FakeNetworkCenter struct {
	fnets map[uint64]*FakeNetwork
}

func NewFakeNetworkCenter(groups map[uint64]bool) *FakeNetworkCenter {
	fcenter := &FakeNetworkCenter{fnets: make(map[uint64]*FakeNetwork)}
	assert(nil != fcenter)
	for id, _ := range groups {
		fnet := NewFakeNetwork(id)
		assert(nil != fnet)
		fcenter.fnets[id] = fnet
	}
	return fcenter
}

func (fcenter *FakeNetworkCenter) Get(id uint64) *FakeNetwork {
	if fnet, ok := fcenter.fnets[id]; ok {
		assert(nil != fnet)
		return fnet
	}
	return nil
}

func (fcenter *FakeNetworkCenter) Run() {
	sendc := make(chan pb.Message, len(fcenter.fnets))
	for _, fnet := range fcenter.fnets {
		go fnet.run(sendc)
	}

	// TODO: maybe
	for {
		rmsg := <-sendc

		if fnet, ok := fcenter.fnets[rmsg.To]; ok {
			fnet.crecvc <- rmsg
		}
	}
}
