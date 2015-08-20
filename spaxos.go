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
	//	sp.rebuildList = make(map[uint64][]pb.Message)

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
	// all spaxos instance in (minIndex, maxIndex] must
	// already in insgroup, or don't reach this spaxos yet;
	if index > sp.maxIndex {
		// new spaxos instance: cli prop
		ins = newSpaxosInstance(index)
		assert(nil != ins)
		sp.insertAndCheck(ins)

		LogDebug("inc sp.maxIndex from %d to %d", sp.maxIndex, index)
		sp.maxIndex = index
		return ins
	} else if index <= sp.minIndex {
		// index <= sp.minIndex: indicate it's a chosen spaxos instance
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
	// => stepNilSpaxosInstance: deal with index < sp.minIndex
	// => which in turn must be chosen spaxos instance
	assert(msg.Index <= sp.minIndex)
	assert(0 < msg.Index)
	if sp.id == msg.From {
		return // ignore
	}

	// readMsg will be process by storage thread
	readMsg := pb.Message{
		Type: pb.MsgReadChosen, Index: msg.Index,
		From: sp.id, To: msg.From}
	sp.appendMsg(readMsg)
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

			// generate broad-cast msg if needed
			for _, msg := range outMsgs {
				switch msg.Type {
				case pb.MsgReadChosen:
					assert(msg.Index <= spkg.minIndex)
					hs, err := db.Get(msg.Index)
					if nil != err {
						LogDebug("MsgReadChosen Get %d err %s",
							msg.Index, err)
						continue
					}

					assert(hs.Index == msg.Index)
					chosenMsg := pb.Message{Type: pb.MsgChosen,
						Index: msg.Index, From: msg.From, To: msg.To,
						Entry: pb.PaxosEntry{Value: hs.AcceptedValue}}
					// reset msg
					msg = chosenMsg
				case pb.MsgChosen:
					// ignore dropMsgs
				default:
					if dropMsgs {
						continue
					}
				}

				// msg.To == 0 => indicate a broadcast msg
				if 0 == msg.To {
					bmsgs := sp.generateBroadcastMsgs(msg)
					sendingMsgs = append(sendingMsgs, bmsgs...)
				} else {
					sendingMsgs = append(sendingMsgs, msg)
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
