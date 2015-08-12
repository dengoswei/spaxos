package spaxos

import (
	"bytes"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type storePackage struct {
	outMsgs       []pb.Message
	outHardStates []pb.HardState
}

type spaxos struct {
	// id of myself
	id     uint64
	groups map[uint64]bool

	// current state
	outMsgs       []pb.Message
	outHardStates []pb.HardState
	chosenItem    map[uint64]pb.HardState

	// index & spaxosInstance
	maxIndex uint64
	minIndex uint64
	insgroup map[uint64]*spaxosInstance

	// rebuild spaxosInstance
	rebuildList map[uint64][]pb.Message

	// communicate channel
	// attach: smt & db
	propc   chan pb.Message
	chosenc chan []pb.HardState

	// attach: smt & st
	storec chan storePackage

	// attach: st & nt
	sendc chan []pb.Message

	// attach: smt & nt
	recvc chan pb.Message

	// (0, minIndex) ins in allSps, if not,
	//    => re-build base ond disk hardstate
	// for index in (0, minIndex), will be a fifo queue
	// minIndex uint64
	// maxIndex uint64
	// pb.Message => index => ins need to re-build base on disk hardstate
	// fifoIndex []uint64
	// allSps    map[uint64]*spaxosInstance
	// mySps map[uint64]*spaxosInstance

	// msg handOn: wait for ins re-build;
	// handOn map[uint64][]pb.Message

	//	// index -> ins: need re-build
	//	rebuild []uint64 // rebuild index
	//	chosen  map[uint64][]byte
	//	hss     []pb.HardState
	//	msgs    []pb.Message
	//
	//	prevHSS []pb.HardState
}

//func newSpaxos(
//	selfid uint64, groupsid []uint64, minIndex, maxIndex uint64) *spaxos {
//	groups := make(map[uint64]bool)
//	for _, id := range groupsid {
//		groups[id] = true
//	}
//
//	if _, ok := groups[selfid]; !ok {
//		// ERROR CASE
//		return nil
//	}
//
//	sp := &spaxos{
//		id:        selfid,
//		groups:    groups,
//		minIndex:  minIndex,
//		maxIndex:  maxIndex,
//		allSps:    make(map[uint64]*spaxosInstance),
//		mySps:     make(map[uint64]*spaxosInstance),
//		handOn:    make(map[uint64][]pb.Message),
//		currState: newSpaxosState()}
//	//		chosen:   make(map[uint64][]byte)}
//	assert(nil != sp)
//	return sp
//}
//

func (sp *spaxos) submitChosen(hs pb.HardState) {
	assert(true == hs.Chosen)

	val, ok := sp.chosenItem[hs.Index]
	if !ok {
		sp.chosenItem[hs.Index] = hs
		return
	}

	// already chosen
	assert(true == val.Chosen)
	assert(val.Index == hs.Index)
	assert(0 == bytes.Compare(val.AcceptedValue, hs.AcceptedValue))
}

func (sp *spaxos) appendMsg(msg pb.Message) {
	assert(sp.id == msg.From)
	sp.outMsgs = append(sp.outMsgs, msg)
}

func (sp *spaxos) appendHardState(hs pb.HardState) {
	sp.outHardStates = append(sp.outHardStates, hs)
}

func (sp *spaxos) getNextProposeNum(prev, hint uint64) uint64 {
	assert(0 != sp.id)
	hint = MaxUint64(prev, hint)
	next := (hint + MaxNodeID - 1) / MaxNodeID * MaxNodeID
	return next + sp.id
}

func (sp *spaxos) asMajority(votes map[uint64]bool, cond bool) bool {
	assert(nil != votes)

	total := len(sp.groups)
	cnt := 0
	for _, b := range votes {
		if b == cond {
			cnt += 1
		}
	}
	return cnt > total/2
}

func (sp *spaxos) Run() {
	// runs state machine

	// TODO
}

// interface to db
// IMPORTANT:
// data, err := pb.ProposeValue{Reqid: reqid, Value: value}.Marshal()
func (sp *spaxos) Propose(data []byte) error {
	propMsg := pb.Message{
		Type:  pb.MsgCliProp,
		Entry: pb.PaxosEntry{Value: data}}
	select {
	case sp.propc <- propMsg:
		// TODO: add timeout ?
	}

	return nil
}

// IMPORTANT:
// map:
// - key: index
// - value:
//   err := pb.ProposeValue{}.Unmarshal(value)
func (sp *spaxos) GetChosenValue() map[uint64][]byte {
	select {
	case chosengroup := <-sp.chosenc:
		items := make(map[uint64][]byte, len(chosengroup))
		for _, hs := range chosengroup {
			assert(true == hs.Chosen)
			assert(uint64(0) < hs.Index)
			items[hs.Index] = hs.AcceptedValue
		}

		return items

		// TODO: add timeout ?
	}

	return nil
}

// TODO: smt, st, nt
