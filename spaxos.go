package spaxos

import (
	"bytes"

	pb "spaxos/spaxospb"
)

type spaxos struct {
	// id of myself
	id     uint64
	groups map[uint64]bool

	// current state
	outMsgs       []pb.Message
	outHardStates []pb.HardState
	chosenItems   map[uint64]pb.HardState

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

	val, ok := sp.chosenItems[hs.Index]
	if !ok {
		sp.chosenItems[hs.Index] = hs
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
// propValue := pb.ProposeValue{Reqid: reqid, Value: value}
// propItem := pb.ProposeItem{Values: append([]pb.ProposeValue, propValue)}
// data, err = propItem.Marshal()
func (sp *spaxos) Propose(data []byte, asMaster bool) error {
	msgType := pb.MsgCliProp
	if asMaster {
		// issue a master propose
		msgType = pb.MsgMCliProp
	}

	// data: may contain multiple <reqid, value>
	propMsg := pb.Message{
		Type:  msgType,
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
//   item := pb.ProposeItem{}
//   err := item.Unmarshal(value)
//   for _, propValue := range item.values {
//       reqid := propValue.Reqid
//       value := propValue.Value
//   }
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

func (sp *spaxos) getChosen() (chan []pb.HardState, []pb.HardState) {
	if 0 == len(sp.chosenItems) {
		return nil, nil
	}

	cits := make([]pb.HardState, len(sp.chosenItems))
	for idx, hs := range sp.chosenItems {
		assert(idx == hs.Index)
		assert(true == hs.Chosen)
		assert(nil != hs.AcceptedValue)
		cits = append(cits, hs)
	}

	assert(len(sp.chosenItems) == len(cits))
	return sp.chosenc, cits
}

func (sp *spaxos) getStroagePackage() (chan storePackage, storePackage) {
	if nil == sp.outMsgs && nil == sp.outHardStates {
		return nil, storePackage{}
	}

	return sp.storec, storePackage{
		outMsgs: sp.outMsgs, outHardStates: sp.outHardStates}
}

// State Machine Threaed
func (sp *spaxos) runStateMachine() {
	var propc chan pb.Message

	myindex := uint64(0)
	for {
		// select on propc only if myindex == 0
		if 0 == myindex {
			propc = sp.propc
		} else {
			propc = nil
		}

		// select on sp.chosenc only when abs needed
		chosenc, cits := sp.getChosen()

		// select on sp.storec only when abs needed
		storec, spkg := sp.getStroagePackage()

		select {
		case propMsg := <-propc:
			assert(nil != propMsg.Entry.Value)
			// assign a index(max+1) & create a new spaxos instace
			// deal with propMsg
			// TODO

		case msg := <-sp.recvc:
			assert(0 != msg.Index)
			// look up the coresponding spaxos instance
			// pass msg to spaxos instance
			// TODO

		case chosenc <- cits:
			// success send out the chosen items
			// clean up the chosenItems state
			sp.chosenItems = make(map[uint64]pb.HardState)

		case storec <- spkg:
			// clean up state
			sp.outMsgs = nil
			sp.outHardStates = nil
		}
	}
}

// Storage Thread
func (sp *spaxos) runStorage(db Storage) {
	assert(nil != db)

	var sendingMsgs []pb.Message
	for {
		// TODO ?
		select {
		case spkg := <-sp.storec:
			// deal with spkg
			// TODO
			outHardStates := spkg.outHardStates
			outMsgs := spkg.outMsgs
			assert(nil != outHardStates || nil != outMsgs)
			// store outhardStates first

			err := db.Store(outHardStates)
			dropMsgs := false
			if nil != err {
				dropMsgs = true
			}

			// deal with rebuild msg ?
			for _, msg := range outMsgs {
				if pb.MsgInsRebuild == msg.Type {
					assert(0 < msg.Index)
					// TODO
					// db.Get()
				} else if !dropMsgs {
					sendingMsgs = append(sendingMsgs, msg)
				}
			}

		case sp.sendc <- sendingMsgs:
			// send out msgs
			sendingMsgs = nil
		}
	}
}

// Network Thread
func (sp *spaxos) runNetwork(net Network) {
}

// TODO: smt, st, nt
