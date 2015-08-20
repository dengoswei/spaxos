package spaxos

import (
	//	"errors"
	"time"

	pb "spaxos/spaxospb"
)

type spaxos struct {
	id     uint64
	groups map[uint64]bool

	// current state
	outMsgs       []pb.Message
	outHardStates []pb.HardState

	nextMinIndex uint64
	chosenMap    map[uint64]bool
	//	chosenItems  map[uint64]pb.HardState

	// index & spaxosInstance
	maxIndex uint64
	minIndex uint64
	insgroup map[uint64]*spaxosInstance

	// rebuild spaxosInstance
	rebuildList map[uint64][]pb.Message

	// signal done
	done chan struct{}
	stop chan struct{}

	// timeout
	elapsed      uint64
	timeoutQueue map[uint64]map[uint64]*spaxosInstance
	tickc        chan struct{}

	// communicate channel
	// attach: smt & db
	propc chan pb.Message

	// TODO:
	// chindexc chan uint64
	// chosenc  chan []pb.HardState

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

func NewSpaxos(id uint64, groups map[uint64]bool) *spaxos {
	assert(0 != id)
	_, ok := groups[id]
	assert(true == ok)

	sp := &spaxos{id: id, groups: groups}

	// signal done
	sp.done = make(chan struct{})
	sp.stop = make(chan struct{})

	// timeout
	sp.timeoutQueue = make(map[uint64]map[uint64]*spaxosInstance)
	assert(nil != sp.timeoutQueue)
	sp.tickc = make(chan struct{})

	sp.chosenMap = make(map[uint64]bool)
	//	sp.chosenItems = make(map[uint64]pb.HardState)
	sp.insgroup = make(map[uint64]*spaxosInstance)
	sp.rebuildList = make(map[uint64][]pb.Message)

	sp.propc = make(chan pb.Message)
	// sp.chosenc = make(chan []pb.HardState)

	// TODO

	sp.storec = make(chan storePackage)
	sp.sendc = make(chan []pb.Message)
	sp.recvc = make(chan pb.Message)

	return sp
}

func (sp *spaxos) Stop() {
	select {
	case sp.stop <- struct{}{}:
	case <-sp.done:
		return
	}

	<-sp.done
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

func (sp *spaxos) submitChosen(index uint64) {
	assert(0 < index)
	assert(nil != sp.chosenMap)
	sp.chosenMap[index] = true
	assert(sp.nextMinIndex >= sp.minIndex)
	if sp.nextMinIndex+1 == index {
		// update nextMinIndex
		newMinIndex := sp.nextMinIndex + 1
		for ; newMinIndex < sp.maxIndex; newMinIndex++ {
			if _, ok := sp.chosenMap[newMinIndex+1]; !ok {
				break
			}
		}
		assert(newMinIndex >= sp.nextMinIndex+1)
		sp.nextMinIndex = newMinIndex
	}
}

//func (sp *spaxos) submitChosen(hs pb.HardState) {
//	assert(true == hs.Chosen)
//	assert(nil != sp.chosenItems)
//
//	val, ok := sp.chosenItems[hs.Index]
//	if !ok {
//		sp.chosenItems[hs.Index] = hs
//		return
//	}
//
//	// already chosen
//	assert(true == val.Equal(&hs))
//}

func (sp *spaxos) appendMsg(msg pb.Message) {
	assert(sp.id == msg.From)
	sp.outMsgs = append(sp.outMsgs, msg)
}

func (sp *spaxos) appendHardState(hs pb.HardState) {
	sp.outHardStates = append(sp.outHardStates, hs)
}

func (sp *spaxos) generateRebuildMsg(index uint64) pb.Message {
	return pb.Message{
		Type: pb.MsgInsRebuild, From: sp.id, To: sp.id, Index: index}
}

func (sp *spaxos) handOnMsg(msg pb.Message) {
	if _, ok := sp.rebuildList[msg.Index]; !ok {
		rebuildMsg := sp.generateRebuildMsg(msg.Index)
		sp.appendMsg(rebuildMsg)
	}

	sp.rebuildList[msg.Index] = append(sp.rebuildList[msg.Index], msg)
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
// func (sp *spaxos) Propose(data []byte, asMaster bool) error {
func (sp *spaxos) multiPropose(
	reqid uint64, values [][]byte, asMaster bool) error {
	assert(0 < reqid)
	assert(0 < len(values))

	msgType := pb.MsgCliProp
	if asMaster {
		// issue a master propose
		msgType = pb.MsgMCliProp
	}

	// data: may contain multiple <reqid, value>
	pitem := &pb.ProposeItem{Reqid: reqid, Values: values}

	propMsg := pb.Message{
		Type: msgType, From: sp.id, To: sp.id,
		Entry: pb.PaxosEntry{Value: pitem}}
	select {
	case sp.propc <- propMsg:
		// TODO: add timeout ?
	}

	return nil
}

func (sp *spaxos) propose(reqid uint64, value []byte, asMaster bool) error {
	return sp.multiPropose(reqid, [][]byte{value}, asMaster)
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
//func (sp *spaxos) GetChosenValue() []pb.HardState {
//	select {
//	case chosengroup := <-sp.chosenc:
//		// do not repeat !!!
//		return chosengroup
//		// TODO: add timeout ?
//	}
//
//	return nil
//}

func (sp *spaxos) GetChosenValueAt(index uint64, db Storager) (pb.HardState, error) {
	// for index < db.ChosenIndex
	// => protocol guarrent: the most recent hs read out of db.Get is chosen paxos instace

	// TODO
	return pb.HardState{}, nil
}

//func (sp *spaxos) GetChosenValueAt(index uint64) ([]byte, error) {
//	if 0 == index {
//		return nil, errors.New("INVALID INDEXI")
//	}
//
//	sp.chindexc <- index
//
//	select {
//	// TODO
//	case hs := <-sp.chvaluec:
//		assert(true == hs.Chosen)
//		assert(index == hs.Index)
//		return hs.AcceptedValue, nil
//		// TODO: timeout ?
//	}
//
//	// TODO
//	return nil, nil
//}

//func (sp *spaxos) getChosen() (chan []pb.HardState, []pb.HardState) {
//	if 0 == len(sp.chosenItems) {
//		return nil, nil
//	}
//
//	cits := []pb.HardState{}
//	for idx, hs := range sp.chosenItems {
//		assert(idx == hs.Index)
//		assert(true == hs.Chosen)
//		assert(nil != hs.AcceptedValue)
//		cits = append(cits, hs)
//	}
//
//	assert(len(sp.chosenItems) == len(cits))
//	return sp.chosenc, cits
//}

func (sp *spaxos) getStorePackage() (chan storePackage, storePackage) {
	if nil == sp.outMsgs && nil == sp.outHardStates {
		return nil, storePackage{}
	}

	return sp.storec, storePackage{
		minIndex: sp.nextMinIndex,
		outMsgs:  sp.outMsgs, outHardStates: sp.outHardStates}
}

// State Machine Threaed
func (sp *spaxos) insertAndCheck(ins *spaxosInstance) {
	assert(0 != ins.index)
	_, ok := sp.insgroup[ins.index]
	assert(false == ok)
	sp.insgroup[ins.index] = ins
}

func (sp *spaxos) getSpaxosInstance(index uint64) *spaxosInstance {
	ins, ok := sp.insgroup[index]
	if ok {
		assert(nil != ins)
		return ins
	}

	assert(nil == ins)
	if index > sp.maxIndex {
		// new spaxos instance: cli prop
		ins = newSpaxosInstance(index)
		assert(nil != ins)
		sp.insertAndCheck(ins)

		LogDebug("inc sp.maxIndex from %d to %d", sp.maxIndex, index)
		sp.maxIndex = index
		return ins
	} else if index <= sp.minIndex {
		// need rebuild
		return nil
	}

	// index >= sp.minIndex && index <= sp.maxIndex
	// propose by peers: simplely create a one
	ins = newSpaxosInstance(index)
	assert(nil != ins)
	sp.insertAndCheck(ins)
	return ins
}

func (sp *spaxos) stepNilSpaxosInstance(msg pb.Message) {
	// not yet available:
	// => hand on this msg & gen rebuild msg if not yet gen-ed
	if pb.MsgInsRebuildResp != msg.Type {
		sp.handOnMsg(msg)
		return
	}

	// rebuild ins
	ins := rebuildSpaxosInstance(msg.Hs)
	assert(nil != ins)
	assert(ins.index == msg.Index)
	// add ins into insgroups
	sp.insertAndCheck(ins)

	prevMsgs, ok := sp.rebuildList[ins.index]
	if ok {
		for _, prevMsg := range prevMsgs {
			sp.step(prevMsg)
		}
	}
}

func (sp *spaxos) updateTimeout(ins *spaxosInstance) {
	assert(nil != ins)
	assert(0 < ins.index)
	// : to avoid live lock
	//  => may add rand into timeout setting !
	newTimeout := sp.elapsed + 10 // default 10ms timeout(TODO: fix)
	if ins.timeoutAt == newTimeout {
		// no update:
		return
	}

	prevTimeout := ins.timeoutAt
	// delete ins from prevAactive timeout queue
	if mapTimeout, ok := sp.timeoutQueue[prevTimeout]; ok {
		if _, ok := mapTimeout[ins.index]; ok {
			delete(mapTimeout, ins.index)
		}
	}

	// add ins into current active timeout queue
	ins.timeoutAt = newTimeout
	if _, ok := sp.timeoutQueue[ins.timeoutAt]; !ok {
		// mapTimeout not yet create
		sp.timeoutQueue[ins.timeoutAt] = make(map[uint64]*spaxosInstance)
	}
	sp.timeoutQueue[ins.timeoutAt][ins.index] = ins
	assert(0 < len(sp.timeoutQueue[ins.timeoutAt]))
}

func (sp *spaxos) updateMinIndex(newMinIndex uint64) {
	if sp.minIndex >= newMinIndex {
		return // do nothing
	}

	assert(newMinIndex <= sp.nextMinIndex)
	for index := sp.minIndex + 1; index <= newMinIndex; index++ {
		delete(sp.insgroup, index)
		delete(sp.chosenMap, index)
		sp.minIndex = index
		LogDebug("updateMinIndex retire index %d", index)
	}
	assert(sp.minIndex == newMinIndex)
}

func (sp *spaxos) step(msg pb.Message) {
	assert(0 != msg.Index)
	assert(sp.id == msg.To)

	if pb.MsgUpdateMinIndex == msg.Type {
		sp.updateMinIndex(msg.Index)
		return
	}

	// get ins by index
	ins := sp.getSpaxosInstance(msg.Index)
	if nil == ins {
		sp.stepNilSpaxosInstance(msg)
		return
	}

	switch msg.Type {
	case pb.MsgCliProp:
		fallthrough
	case pb.MsgMCliProp:
		assert(nil != msg.Entry.Value)
		ins.Propose(sp, msg.Entry.Value, pb.MsgMCliProp == msg.Type)

	default:
		ins.step(sp, msg)
	}
}

func (sp *spaxos) runTick() {
	for {
		select {
		case <-time.After(time.Millisecond * 1):
			sp.tickc <- struct{}{}
		}
	}
}

// FOR TEST
func (sp *spaxos) fakeRunStateMachine() {
	// only to recieve stop signal
	select {
	case <-sp.stop:
		close(sp.done)
		return
	}
}

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
		// chosenc, cits := sp.getChosen()

		// select on sp.storec only when abs needed
		storec, spkg := sp.getStorePackage()

		select {
		case propMsg := <-propc:
			assert(nil != propMsg.Entry.Value)
			propMsg.Index = sp.maxIndex + 1
			LogDebug("prop msg %v", propMsg)
			sp.step(propMsg)

		case msg := <-sp.recvc:
			assert(0 != msg.Index)
			// look up the coresponding spaxos instance
			// pass msg to spaxos instance
			assert(0 != msg.From)
			if sp.id == msg.To {
				sp.step(msg)
			}

			//	case chosenc <- cits:
			//		// success send out the chosen items
			//		// clean up the chosenItems state
			//		sp.chosenItems = make(map[uint64]pb.HardState)

		case storec <- spkg:
			// clean up state
			sp.outMsgs = nil
			sp.outHardStates = nil

		case <-sp.tickc:
			sp.elapsed++
			assert(nil != sp.timeoutQueue)
			if mapIns, ok := sp.timeoutQueue[sp.elapsed]; ok {
				for idx, timeoutIns := range mapIns {
					assert(nil != timeoutIns)
					assert(idx == timeoutIns.index)

					timeoutMsg := generateTimeoutMsg(idx, sp.id, sp.elapsed)
					sp.step(timeoutMsg)
				}

				delete(sp.timeoutQueue, sp.elapsed)
			}

		case <-sp.stop:
			close(sp.done)
			return
		}
	}
}

func (sp *spaxos) generateBroadcastMsgs(msg pb.Message) []pb.Message {
	var msgs []pb.Message
	for id, _ := range sp.groups {
		if id != sp.id {
			newmsg := msg
			newmsg.To = id
			msgs = append(msgs, newmsg)
		}
	}

	return msgs
}

// Storage Thread
func (sp *spaxos) runStorage(db Storager) {
	assert(nil != db)

	var sendingMsgs []pb.Message
	for {

		select {
		case spkg := <-sp.storec:
			// deal with spkg
			outHardStates := spkg.outHardStates
			outMsgs := spkg.outMsgs
			assert(nil != outHardStates || nil != outMsgs)
			// store outhardStates first

			// shrink outHardStates
			{
				mapHardStates := make(map[uint64]pb.HardState)
				for _, hs := range outHardStates {
					mapHardStates[hs.Index] = hs
				}

				outHardStates = nil
				for _, hs := range mapHardStates {
					outHardStates = append(outHardStates, hs)
				}
			}

			dropMsgs := false
			if nil != outHardStates {
				err := db.Store(outHardStates)
				if nil != err {
					dropMsgs = true
				}
			}

			// tell runStateMachine to update minIndex
			if false == dropMsgs {
				err := db.SetMinIndex(spkg.minIndex)
				if nil == err {
					indexMsg := pb.Message{
						Type: pb.MsgUpdateMinIndex, Index: spkg.minIndex,
						From: sp.id, To: sp.id}
					sendingMsgs = append(sendingMsgs, indexMsg)
				}
			}

			// deal with rebuild msg
			// generate broad-cast msg if needed
			for _, msg := range outMsgs {
				if pb.MsgInsRebuild == msg.Type {
					assert(0 < msg.Index)
					assert(sp.id == msg.From)
					assert(sp.id == msg.To)

					rspMsg := pb.Message{
						Type:   pb.MsgInsRebuildResp,
						Index:  msg.Index,
						From:   sp.id,
						To:     sp.id,
						Reject: false,
					}

					hs, err := db.Get(msg.Index)
					if nil != err {
						rspMsg.Reject = true
					} else {
						rspMsg.Hs = hs
					}

					sendingMsgs = append(sendingMsgs, rspMsg)
				} else if !dropMsgs {
					if uint64(0) == msg.To {
						// broadcast but myself
						bmsgs := sp.generateBroadcastMsgs(msg)
						sendingMsgs = append(sendingMsgs, bmsgs...)
					} else {
						sendingMsgs = append(sendingMsgs, msg)
					}
				}
			}

		case sp.sendc <- sendingMsgs:
			// send out msgs
			sendingMsgs = nil

		case <-sp.done:
			return
		}
	}
}

// Network Thread
func (sp *spaxos) runNetwork(net Networker) {
	var sendingMsgs []pb.Message
	var forwardingMsgs []pb.Message

	nrecvc := net.GetRecvChan()
	for {
		nsendc, smsg := getMsg(net.GetSendChan(), sendingMsgs)
		if nil != nsendc {
			assert(sp.id == smsg.From)
		}

		forwardc, fmsg := getMsg(sp.recvc, forwardingMsgs)
		if nil != forwardc {
			assert(sp.id == fmsg.To)
		}

		select {
		// collect msg from network recvc
		case msg := <-nrecvc:
			assert(0 < msg.Index)
			if msg.To == sp.id {
				forwardingMsgs = append(forwardingMsgs, msg)
			}

		// collect msgs from st
		case msgs := <-sp.sendc:
			for _, msg := range msgs {
				assert(0 < msg.Index)
				assert(sp.id == msg.From)
				if msg.To == sp.id {
					// forwarding msg
					forwardingMsgs = append(forwardingMsgs, msg)
				} else {
					sendingMsgs = append(sendingMsgs, msg)
				}
			}
			LogDebug("msgs %d forwardingMsgs %d sendingMsgs %d",
				len(msgs), len(forwardingMsgs), len(sendingMsgs))

		// sending msg only when needed
		case nsendc <- smsg:
			sendingMsgs = sendingMsgs[1:]
			LogDebug("sending msg %v", smsg)

		// forwarding msg only when needed
		case forwardc <- fmsg:
			forwardingMsgs = forwardingMsgs[1:]

		case <-sp.done:
			return
		}
	}
}
