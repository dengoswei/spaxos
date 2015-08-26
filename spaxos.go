package spaxos

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	//	pb "spaxos/spaxospb"
	pb "github.com/dengoswei/spaxos/spaxospb"
)

var (
	ErrStopped = errors.New("spaxos: stopped")
)

const heartBeatInterval = uint64(100) // 100ms

type spaxos struct {
	id     uint64
	groups map[uint64]bool

	// current state
	outMsgs       []pb.Message
	outHardStates []pb.HardState

	nextMinIndex uint64
	chosenMap    map[uint64]bool

	// index & spaxosInstance
	maxIndex uint64
	minIndex uint64
	insgroup map[uint64]*spaxosInstance

	// peers maxIndex info
	// peersMaxIndex map[uint64]uint64

	// signal done
	done chan struct{}
	stop chan struct{}

	// timeout
	elapsed       uint64
	prevHeartBeat uint64
	timeoutQueue  map[uint64]map[uint64]*spaxosInstance
	tickc         chan struct{}

	// communicate channel
	// attach: front-end & smt
	propc   chan pb.Message
	notifyc chan struct{}

	// attach: smt & st
	storec chan storePackage

	// attach: st & nt
	sendc chan []pb.Message

	// attach: smt & nt
	recvc chan pb.Message

	// timeout setting
	heartBeatInterval uint64
	instanceTimeout   uint64
}

func newSpaxos(c *Config, db Storager) (*spaxos, error) {
	assert(nil != c)

	rand.Seed(time.Now().UnixNano())
	sp := &spaxos{
		id:           c.Selfid,
		groups:       c.GetGroupIds(),
		done:         make(chan struct{}),
		stop:         make(chan struct{}),
		timeoutQueue: make(map[uint64]map[uint64]*spaxosInstance),
		tickc:        make(chan struct{}),
		chosenMap:    make(map[uint64]bool),
		insgroup:     make(map[uint64]*spaxosInstance),
		//		peersMaxIndex: make(map[uint64]uint64),
		propc:             make(chan pb.Message),
		notifyc:           make(chan struct{}),
		storec:            make(chan storePackage),
		sendc:             make(chan []pb.Message),
		recvc:             make(chan pb.Message),
		heartBeatInterval: c.Stimeout.Heartbeat,
		instanceTimeout: c.Stimeout.Instimeoutbase +
			uint64(rand.Intn(int(c.Stimeout.Instimeoutrange)))}
	assert(0 < sp.heartBeatInterval)
	assert(0 < sp.instanceTimeout)

	minIndex, maxIndex, err := db.GetIndex()
	if nil != err {
		fmt.Printf("db.GetIndex err %s\n", err)
		return nil, err
	}

	sp.minIndex = minIndex
	sp.maxIndex = maxIndex
	sp.nextMinIndex = minIndex
	assert(sp.minIndex <= sp.maxIndex)
	for index := sp.minIndex + 1; index <= sp.maxIndex; index++ {
		hs, err := db.Get(index)
		if nil != err {
			if IndexNotExist == err {
				continue
			}

			return nil, err
		}

		assert(index == hs.Index)
		ins := rebuildSpaxosInstance(hs)
		assert(nil != ins)
		assert(ins.index == index)
		sp.insertAndCheck(ins)
		if ins.chosen {
			sp.submitChosen(ins.index)
		}
	}

	sp.generateHeartBeatMsg()
	// TODO:
	// 1. catch up ?
	// 2. noop on last maxIndex(no-chosen) ?
	return sp, nil
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
	LogDebug("appendMsg host id %d msg (type %s) %v",
		sp.id, pb.MessageType_name[int32(msg.Type)], msg)
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

func (sp *spaxos) recvAllRsp(votes map[uint64]bool) bool {
	assert(nil != votes)
	return len(sp.groups) == len(votes)
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
		LogDebug("%s msg %v", GetFunctionName(sp.multiPropose), propMsg)
	case <-sp.done:
		return ErrStopped
	}

	return nil
}

func (sp *spaxos) waitProposeRsp() error {
	// TODO
	return nil
}

func (sp *spaxos) propose(reqid uint64, value []byte, asMaster bool) error {
	return sp.multiPropose(reqid, [][]byte{value}, asMaster)
}

func (sp *spaxos) tryCatchUp() error {
	catchUpMsg := pb.Message{
		Type: pb.MsgTryCatchUp, From: sp.id, To: sp.id}
	select {
	case sp.recvc <- catchUpMsg:
		LogDebug("%s msg %v", GetFunctionName(sp.tryCatchUp), catchUpMsg)
	case <-sp.done:
		return ErrStopped
	}
	return nil
}

func (sp *spaxos) getStorePackage() (chan storePackage, storePackage) {
	if nil == sp.outMsgs && nil == sp.outHardStates {
		return nil, storePackage{}
	}

	return sp.storec, storePackage{
		minIndex: sp.nextMinIndex, maxIndex: sp.maxIndex,
		outMsgs: sp.outMsgs, outHardStates: sp.outHardStates}
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

	switch msg.Type {
	case pb.MsgProp, pb.MsgAccpt:
		// readMsg will be process by storage thread
		readMsg := pb.Message{
			Type: pb.MsgReadChosen, Index: msg.Index,
			From: sp.id, To: msg.From}
		sp.appendMsg(readMsg)
		return
	default:
		LogDebug("%s ignore msg %v", GetCurrentFuncName(), msg)
	}

	return
}

func (sp *spaxos) generateHeartBeatMsg() {
	heartBeatMsg := pb.Message{
		Type: pb.MsgBeat, Index: sp.maxIndex,
		From: sp.id, To: 0}
	sp.appendMsg(heartBeatMsg)
	sp.prevHeartBeat = sp.elapsed
	LogDebug("hostid %d generateHeartBeatMsg elapsed %d", sp.id, sp.elapsed)
}

func (sp *spaxos) updateTimeout(ins *spaxosInstance) {
	assert(nil != ins)
	assert(0 < ins.index)
	// : to avoid live lock
	//  => may add rand into timeout setting !
	// TODO: add random
	assert(0 < sp.instanceTimeout)
	newTimeout := sp.elapsed + sp.instanceTimeout
	//	newTimeout := sp.elapsed + 10 // default 10ms timeout(TODO: fix)
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
		LogDebug("updateMinIndex hostid %d retire index %d", sp.id, index)
	}
	assert(sp.minIndex == newMinIndex)
}

func (sp *spaxos) doTryCatchUp(msg pb.Message) {
	assert(sp.id == msg.To)
	const catchUpStep = 10

	assert(sp.minIndex <= sp.maxIndex)
	for i := 0; i < catchUpStep; i++ {
		catchIndex := sp.minIndex + uint64(i+1)
		if catchIndex > sp.maxIndex {
			break
		}

		if _, ok := sp.insgroup[catchIndex]; ok {
			continue
		}

		// catchIndex don't have spaxos instance yet
		catchIns := sp.getSpaxosInstance(catchIndex)
		assert(nil != catchIns)
		// => create a new spaxos instance for catchIndex

		msg.Index = catchIndex
		catchIns.step(sp, msg)
	}

	//	// use heartbeat msg: ask for
	//	if sp.maxIndex-sp.minIndex == 0 {
	//		// most recent
	//		heartBeatMsg := pb.Message{Type: pb.MsgBeat, From: sp.id, To: 0}
	//		sp.appendMsg(heartBeatMsg)
	//	}
}

func (sp *spaxos) updateStatus(msg pb.Message) {
	assert(sp.id == msg.To)
	if msg.Index > sp.maxIndex {
		LogDebug("%s update maxIndex %d => %d",
			GetCurrentFuncName(), sp.maxIndex, msg.Index)
		sp.maxIndex = msg.Index
	}
}

//func (sp *spaxos) updateStatus(msg pb.Message) {
//	assert(sp.id == msg.To)
//
//	// updateStatus: only deal with maxIndex for now!
//	switch msg.Type {
//	case pb.MsgStatus:
//		// ask for msg status
//		statusMsg := pb.Message{Type: pb.MsgStatusResp,
//			Index: sp.maxIndex, From: sp.id, To: msg.From}
//		sp.appendMsg(statusMsg)
//	case pb.MsgStatusResp:
//		peerMaxIndex := msg.Index
//		if sp.maxIndex < peerMaxIndex {
//			LogDebug("%s update maxIndex %d to %d",
//				GetFunctionName(sp.updateStatus),
//				sp.maxIndex, peerMaxIndex)
//			sp.maxIndex = peerMaxIndex
//		}
//	default:
//		assert(false)
//	}
//}

func (sp *spaxos) stepUtiltyMsg(msg pb.Message) {
	assert(sp.id == msg.To)
	switch msg.Type {
	case pb.MsgUpdateMinIndex:
		sp.updateMinIndex(msg.Index)
	case pb.MsgTryCatchUp:
		sp.doTryCatchUp(msg)
	case pb.MsgBeat:
		sp.updateStatus(msg)
		//	case pb.MsgStatus, pb.MsgStatusResp:
		//		sp.updateStatus(msg)
	default:
		assert(false)
	}
	return
}

func (sp *spaxos) step(msg pb.Message) {
	assert(sp.id == msg.To)

	switch msg.Type {
	case pb.MsgUpdateMinIndex, pb.MsgTryCatchUp:
		fallthrough
	case pb.MsgBeat:
		sp.stepUtiltyMsg(msg)
		return
		//	case pb.MsgStatus, pb.MsgStatusResp:
		//		sp.stepUtiltyMsg(msg)
		//		return
	// TODO
	default:
	}

	assert(0 != msg.Index)
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
		assert(nil != ins)
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

//func (sp *spaxos) allocateIndexNum() uint64 {
//	if 0 == sp.maxIndex ||
//		sp.minIndex == sp.maxIndex {
//		return sp.maxIndex + 1
//	}
//
//	assert(0 < sp.maxIndex)
//	ins, ok := sp.insgroup[sp.maxIndex]
//	assert(true == ok)
//	assert(nil != ins)
//	assert(ins.index == sp.maxIndex)
//
//	// restriction: only propose after the prev one has been chosen:
//	// MAYBE: if relex this restriction, we can given each proposing
//	// a new index number with a hard limit on how many unchosen spaxos
//	// instance can run at the same time.
//	if true == ins.chosen {
//		return sp.maxIndex + 1
//	}
//
//	assert(false == ins.chosen)
//	// never call allocateIndexNum twice
//	// on the same spaxos instance
//	assert(0 == ins.hostPropReqid)
//	return sp.maxIndex
//}

func (sp *spaxos) runStateMachine() {
	var propc chan pb.Message

	for {
		// select on propc only if spaxos believe it's up-to-date
		if sp.minIndex == sp.maxIndex {
			propc = sp.propc
		} else {
			propc = nil
		}

		// select on sp.storec only when abs needed
		storec, spkg := sp.getStorePackage()

		select {
		case propMsg := <-propc:
			assert(nil != propMsg.Entry.Value)
			assert(sp.minIndex == sp.maxIndex)
			propMsg.Index = sp.maxIndex + 1
			// propMsg.Index = sp.allocateIndexNum()
			assert(0 < propMsg.Index)
			// assert(propMsg.Index >= sp.maxIndex)
			LogDebug("prop msg %v", propMsg)
			sp.step(propMsg)

		case msg := <-sp.recvc:
			assert(0 < msg.Index || pb.MsgBeat == msg.Type)
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

			assert(0 < sp.heartBeatInterval)
			if sp.prevHeartBeat+sp.heartBeatInterval <= sp.elapsed {
				sp.generateHeartBeatMsg()
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
	var notifyc chan struct{}
	prevMinIndex := uint64(0)
	doNotify := false
	for {

		sendc := getMsgs(sp.sendc, sendingMsgs)
		if doNotify {
			notifyc = sp.notifyc
		} else {
			notifyc = nil
		}

		select {
		case notifyc <- struct{}{}:
			doNotify = false

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
					LogDebug("store hostid %d hardstate (host reqid %d) %v",
						sp.id, hs.HostPropReqid, hs)
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
				err := db.SetIndex(spkg.minIndex, spkg.maxIndex)
				if nil == err && prevMinIndex != spkg.minIndex {
					doNotify = true
					indexMsg := pb.Message{
						Type: pb.MsgUpdateMinIndex, Index: spkg.minIndex,
						From: sp.id, To: sp.id}
					sendingMsgs = append(sendingMsgs, indexMsg)
				}
				prevMinIndex = spkg.minIndex
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

		case sendc <- sendingMsgs:
			// send out msgs
			sendingMsgs = nil

		case <-sp.done:
			return
		}
	}
}

// swwork Thread
func (sp *spaxos) runSwitch(sw Switcher) {
	var sendingMsgs []pb.Message
	var forwardingMsgs []pb.Message

	nrecvc := sw.GetRecvChan()
	for {
		nsendc, smsg := getMsg(sw.GetSendChan(), sendingMsgs)
		if nil != nsendc {
			assert(sp.id == smsg.From)
		}

		forwardc, fmsg := getMsg(sp.recvc, forwardingMsgs)
		if nil != forwardc {
			assert(sp.id == fmsg.To)
		}

		select {
		// collect msg from swwork recvc
		case msg := <-nrecvc:
			assert(0 < msg.Index || pb.MsgBeat == msg.Type)
			if msg.To == sp.id {
				forwardingMsgs = append(forwardingMsgs, msg)
			}

		// collect msgs from st
		case msgs := <-sp.sendc:
			for _, msg := range msgs {
				assert(0 < msg.Index || pb.MsgBeat == msg.Type)
				assert(sp.id == msg.From)
				if msg.To == sp.id {
					// forwarding msg
					forwardingMsgs = append(forwardingMsgs, msg)
				} else {
					sendingMsgs = append(sendingMsgs, msg)
				}
			}
			LogDebug("%s msgs %d forwardingMsgs %d sendingMsgs %d",
				GetFunctionName(sp.runSwitch),
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
