package spaxos

import (
	// "errors"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type storePackage struct {
	// chosenIndex: mark all index below as chosen!
	minIndex      uint64
	maxIndex      uint64
	outMsgs       []pb.Message
	outHardStates []pb.HardState
}

type Storager interface {
	// store hard state
	Store([]pb.HardState) error

	Get(index uint64) (*pb.HardState, error)

	SetIndex(minIndex, maxIndex uint64) error
	GetIndex() (uint64, uint64, error)
}

// TODO: fix interface func & name!!!
type Networker interface {
	GetSendChan() chan pb.Message
	GetRecvChan() chan pb.Message
}

type FakeStorage struct {
	minIndex uint64
	maxIndex uint64
	table    map[uint64]pb.HardState
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

func (store *FakeStorage) SetIndex(minIndex, maxIndex uint64) error {
	if minIndex > store.minIndex {
		store.minIndex = minIndex
		LogDebug("%s store.minIndex %d minIndex %d",
			GetFunctionName(store.SetIndex), store.minIndex, minIndex)
	}

	if maxIndex > store.maxIndex {
		store.maxIndex = maxIndex
		LogDebug("%s store.maxIndex %d maxIndex %d",
			GetFunctionName(store.SetIndex), store.maxIndex, maxIndex)
	}

	return nil
}

func (store *FakeStorage) GetIndex() (uint64, uint64, error) {
	return store.minIndex, store.maxIndex, nil
}

func (store *FakeStorage) Get(index uint64) (*pb.HardState, error) {
	assert(0 < index)
	if hs, ok := store.table[index]; ok {
		assert(hs.Index == index)
		if hs.Index <= store.minIndex {
			hs.Chosen = true
		}

		return &hs, nil
	}

	// don't treat not exist as error
	return nil, nil
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

func (fnet *FakeNetwork) run(sendc chan pb.Message, done chan struct{}) {
	for {
		dsendc, smsg := getMsg(sendc, fnet.sendingMsgs)
		drecvc, rmsg := getMsg(fnet.frecvc, fnet.recvingMsgs)

		// TODO
		// drop if hold too many sendingMsgs or recvingMsgs ?
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

		case <-done:
			return
		}
	}
}

type FakeNetworkCenter struct {
	stop  chan struct{}
	done  chan struct{}
	fnets map[uint64]*FakeNetwork
}

func NewFakeNetworkCenter(groups map[uint64]bool) *FakeNetworkCenter {
	fcenter := &FakeNetworkCenter{
		fnets: make(map[uint64]*FakeNetwork),
		stop:  make(chan struct{}),
		done:  make(chan struct{})}
	assert(nil != fcenter)
	for id, _ := range groups {
		fnet := NewFakeNetwork(id)
		assert(nil != fnet)
		fcenter.fnets[id] = fnet
	}
	return fcenter
}

func (fcenter *FakeNetworkCenter) Stop() {
	select {
	case fcenter.stop <- struct{}{}:
	case <-fcenter.done:
		return
	}

	<-fcenter.done
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

	// fan in: => sendc
	for _, fnet := range fcenter.fnets {
		go fnet.run(sendc, fcenter.done)
	}

	for {
		select {
		case rmsg := <-sendc:
			if fnet, ok := fcenter.fnets[rmsg.To]; ok {
				select {
				case fnet.crecvc <- rmsg:
				case <-fcenter.stop:
					close(fcenter.done)
					return
				}
			}
		case <-fcenter.stop:
			close(fcenter.done)
			return
		}
	}
}
