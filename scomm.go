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
type Switcher interface {
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

type FakeSwitch struct {
	id uint64
	// attach to fron-end
	fsendc chan pb.Message
	frecvc chan pb.Message

	// attach to real communication channel
	crecvc      chan pb.Message
	sendingMsgs []pb.Message
	recvingMsgs []pb.Message
}

func NewFakeSwitch(id uint64) *FakeSwitch {
	fswitch := &FakeSwitch{
		id:     id,
		fsendc: make(chan pb.Message),
		frecvc: make(chan pb.Message),
		crecvc: make(chan pb.Message)}
	return fswitch
}

func (fswitch *FakeSwitch) GetSendChan() chan pb.Message {
	return fswitch.fsendc
}

func (fswitch *FakeSwitch) GetRecvChan() chan pb.Message {
	return fswitch.frecvc
}

func (fswitch *FakeSwitch) run(sendc chan pb.Message, done chan struct{}) {
	for {
		dsendc, smsg := getMsg(sendc, fswitch.sendingMsgs)
		drecvc, rmsg := getMsg(fswitch.frecvc, fswitch.recvingMsgs)

		// TODO
		// drop if hold too many sendingMsgs or recvingMsgs ?
		select {
		// send msg in sendingMsgsQueue to sendc
		case dsendc <- smsg:
			fswitch.sendingMsgs = fswitch.sendingMsgs[1:]

		// recv msg from recv append into recvingMsgsQueue
		case msg := <-fswitch.crecvc:
			assert(fswitch.id == msg.To)
			fswitch.recvingMsgs = append(fswitch.recvingMsgs, msg)

		// send msg in recvingMsgsQueue to fswitch.frecvc
		case drecvc <- rmsg:
			fswitch.recvingMsgs = fswitch.recvingMsgs[1:]

			// recv msg from fswitch.fsendc, apppend into sendingMsgsQueue
		case msg := <-fswitch.fsendc:
			fswitch.sendingMsgs = append(fswitch.sendingMsgs, msg)

		case <-done:
			return
		}
	}
}

type FakeSwitchCenter struct {
	stop     chan struct{}
	done     chan struct{}
	fswitchs map[uint64]*FakeSwitch
}

func NewFakeSwitchCenter(groups map[uint64]bool) *FakeSwitchCenter {
	fcenter := &FakeSwitchCenter{
		fswitchs: make(map[uint64]*FakeSwitch),
		stop:     make(chan struct{}),
		done:     make(chan struct{})}
	assert(nil != fcenter)
	for id, _ := range groups {
		fswitch := NewFakeSwitch(id)
		assert(nil != fswitch)
		fcenter.fswitchs[id] = fswitch
	}
	return fcenter
}

func (fcenter *FakeSwitchCenter) Stop() {
	select {
	case fcenter.stop <- struct{}{}:
	case <-fcenter.done:
		return
	}

	<-fcenter.done
}

func (fcenter *FakeSwitchCenter) Get(id uint64) *FakeSwitch {
	if fswitch, ok := fcenter.fswitchs[id]; ok {
		assert(nil != fswitch)
		return fswitch
	}
	return nil
}

func (fcenter *FakeSwitchCenter) Run() {
	sendc := make(chan pb.Message, len(fcenter.fswitchs))

	// fan in: => sendc
	for _, fswitch := range fcenter.fswitchs {
		go fswitch.run(sendc, fcenter.done)
	}

	for {
		select {
		case rmsg := <-sendc:
			if fswitch, ok := fcenter.fswitchs[rmsg.To]; ok {
				select {
				case fswitch.crecvc <- rmsg:
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
