package spaxos

import (
	"errors"
	"log"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type stepSpaxosFunc func(sp *spaxos, msg pb.Message)

type spaxosInstance struct {
	chosen bool
	index  uint64
	// proposer
	maxProposedNum     uint64
	maxAcceptedHintNum uint64
	proposingValue     []byte
	rspVotes           map[uint64]bool
	stepProp           stepSpaxosFunc

	// acceptor
	promisedNum   uint64
	acceptedNum   uint64
	acceptedValue []byte
	stepAccpt     stepSpaxosFunc
}

func getMsgRespType(msgType pb.MessageType) pb.MessageType {
	switch msgType {
	case pb.MsgProp:
		return pb.MsgPropResp
	case pb.MsgAccpt:
		return pb.MsgAccptResp
	}
	return pb.MsgInvalid
}

func (ins *spaxosInstance) getHardState() pb.HardState {
	return pb.HardState{
		MaxProposedNum: ins.maxProposedNum,
		MaxPromisedNum: ins.promisedNum,
		MaxAcceptedNum: ins.acceptedNum,
		AcceptedValue:  ins.acceptedValue}
}

func getDefaultRspMsg(msg pb.Message) pb.Message {
	return pb.Message{Reject: false,
		Index: msg.Index, From: msg.To, To: msg.From,
		Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}
}

func (ins *spaxosInstance) updatePromised(msg pb.Message) pb.Message {
	rsp := getDefaultRspMsg(msg)
	rsp.Type = pb.MsgPropResp
	if ins.promisedNum > msg.Entry.PropNum {
		rsp.Reject = true
		return rsp
	}

	if nil != ins.acceptedValue {
		rsp.Entry.AccptNum = ins.acceptedNum
		rsp.Entry.Value = ins.acceptedValue
	}

	ins.promisedNum = msg.Entry.PropNum
	return rsp
}

func (ins *spaxosInstance) updateAccepted(msg pb.Message) pb.Message {
	rsp := getDefaultRspMsg(msg)
	rsp.Type = pb.MsgAccptResp
	if ins.promisedNum > msg.Entry.PropNum {
		rsp.Reject = true
		return rsp
	}

	assert(nil != msg.Entry.Value)
	ins.promisedNum = msg.Entry.PropNum
	ins.acceptedNum = msg.Entry.PropNum
	ins.acceptedValue = msg.Entry.Value
	return rsp
}

func (ins *spaxosInstance) stepPromised(
	sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	assert(pb.MsgProp == msg.Type)

	rsp := ins.updatePromised(msg)
	sp.appendMsg(rsp)
	if false == rsp.Reject {
		sp.appendHardState(ins.getHardState())
	}
}

func (ins *spaxosInstance) stepAccepted(
	sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	assert(pb.MsgAccpt == msg.Type)

	rsp := ins.updateAccepted(msg)
	sp.appendMsg(rsp)
	if false == rsp.Reject {
		sp.appendHardState(ins.getHardState())
	}
}

// proposer
func (ins *spaxosInstance) beginPreparePhase(sp *spaxos) {
	assert(nil != sp)
	if ins.chosen {
		ins.markChosen(sp)
		return
	}

	// inc ins.maxProposedNum
	nextProposeNum := sp.getNextProposeNum(
		ins.maxProposedNum, ins.promisedNum)

	req := pb.Message{
		Type: pb.MsgProp, Index: ins.index, From: sp.id,
		Entry: pb.PaxosEntry{PropNum: nextProposeNum}}

	rsp := ins.updatePromised(req)
	assert(false == rsp.Reject)
	assert(rsp.From == req.From)

	ins.rspVotes = make(map[uint64]bool)
	ins.rspVotes[sp.id] = true
	ins.maxProposedNum = nextProposeNum
	ins.stepProp = ins.stepPrepareRsp

	sp.appendMsg(req)
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) beginAcceptPhase(sp *spaxos) {
	assert(nil != sp)
	// TODO:
	// accepted action when ins is chosen ?
	req := pb.Message{
		Type: pb.MsgAccpt, Index: ins.index, From: sp.id,
		Entry: pb.PaxosEntry{
			PropNum: ins.maxProposedNum, Value: ins.proposingValue}}

	rsp := ins.updateAccepted(req)
	if true == rsp.Reject {
		// reject by self
		// => backoff to beginPreparePhase
		// NOTE: this may cause live lock
		ins.beginPreparePhase(sp)
		return
	}

	assert(false == rsp.Reject)
	assert(rsp.From == req.From)

	ins.rspVotes = make(map[uint64]bool)
	ins.rspVotes[sp.id] = true
	ins.stepProp = ins.stepAcceptRsp

	sp.appendMsg(req)
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) markChosen(sp *spaxos) {
	assert(nil != sp)

	ins.chosen = true
	sp.submitChosen(ins)
	ins.stepProp = ins.stepChosen
}

func (ins *spaxosInstance) stepPrepareRsp(
	sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)

	// TODO
	// only deal with MsgPropResp msg;
	if pb.MsgPropResp != msg.Type ||
		ins.maxProposedNum != msg.Entry.PropNum {
		return // ignore the mismatch prop num msg
	}

	if val, ok := ins.rspVotes[msg.From]; ok {
		// inconsist !
		assert(val == msg.Reject)
		return
	}

	ins.rspVotes[msg.From] = !msg.Reject
	if false == msg.Reject {
		// update the maxAcceptedHitNum & proposingValue
		if ins.maxAcceptedHintNum < msg.Entry.AccptNum {
			ins.maxAcceptedHintNum = msg.Entry.AccptNum
			ins.proposingValue = msg.Entry.Value
			assert(nil != ins.proposingValue)
		}
	}

	if ins.trueByMajority(ins.rspVotes) {
		ins.beginAcceptPhase(sp)
	} else if ins.falseByMajority(ins.rspVotes) {
		ins.beginPreparePhase(sp)
	}
}

func (ins *spaxosInstance) stepAcceptRsp(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)

	// TODO
	// only deal with MsgAccptResp msg;
	if pb.MsgAccptResp != msg.Type ||
		ins.maxProposedNum != msg.Entry.PropNum {
		return // ignore the mismatch prop num msg
	}

	if val, ok := ins.rspVotes[msg.From]; ok {
		// inconsist !
		assert(val == msg.Reject)
		return
	}

	ins.rspVotes[msg.From] = !msg.Reject
	if ins.trueByMajority(ins.rspVotes) {
		ins.markChosen(sp)
	} else if ins.falseByMajority(ins.rspVotes) {
		ins.beginPreparePhase(sp)
	}
}

func (ins *spaxosInstance) stepChosen(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	assert(true == ins.chosen)

	switch msg.Type {
	// TODO
	}
}

func (ins *spaxosInstance) Step(sp *spaxos, msg pb.Message) {
	assert(msg.Index == ins.index)
	assert(false == ins.chosen)
	assert(ins.acceptedNum <= ins.promisedNum)

	rsp := pb.Message{
		Type: getMsgRespType(msg.Type), Reject: false,
		Index: msg.Index, From: msg.To, To: msg.From,
	}

	switch msg.Type {
	// accepter
	case pb.MsgProp:
		fallthrough
	case pb.MsgAccpt:
		if msg.Entry.PropNum < ins.promisedNum {
			rsp.Reject = true
			sp.appendMsg(rsp)
			return
		}

		ins.promisedNum = msg.Entry.PropNum
		if msg.Type == pb.MsgProp &&
			nil != ins.acceptedValue {
			rsp.Entry.AccptNum = ins.acceptedNum
			rsp.Entry.Value = ins.acceptedValue
		}

		if msg.Type == pb.MsgAccpt {
			assert(nil != msg.Entry.Value)
			ins.acceptedNum = msg.Entry.PropNum
			ins.acceptedValue = msg.Entry.Value
		}

		sp.appendMsg(rsp)
		sp.appendHardState(pb.HardState{
			MaxProposedNum: ins.maxProposedNum,
			MaxPromisedNum: ins.promisedNum,
			MaxAcceptedNum: ins.acceptedNum,
			AcceptedValue:  ins.acceptedValue})

		// proposer
	case pb.MsgPropResp:
		if ins.maxProposedNum != msg.Entry.PropNum {
			return
		}

		// TODO
		ins.pRspVotes[msg.From] = !msg.Reject

	case pb.MsgAccptResp:
	}

}

func (ins *spaxosInstance) trueByMajority(votes map[uint64]bool) bool {
	total := len(ins.sp.groups)

	trueCnt := 0
	for _, b := range votes {
		if b {
			trueCnt += 1
		}
	}

	return trueCnt > total/2
}

func (ins *spaxosInstance) falseByMajority(votes map[uint64]bool) bool {
	total := len(ins.sp.groups)

	falseCnt := 0
	for _, b := range votes {
		if !b {
			falseCnt += 1
		}
	}

	return falseCnt > total/2
}

func (sp *spaxos) getNextProposeNum(prev, hint uint64) uint64 {
	assert(0 != sp.id)
	hint = max(prev, hint)
	next := (hint + MaxNodeID - 1) / MaxNodeID * MaxNodeID
	return next + sp.id
}

type spaxosState struct {
	// index -> ins: need re-build
	rebuild []uint64 // rebuild index
	chosen  map[uint64][]byte
	hss     []pb.HardState
	msgs    []pb.Message

	prevHSS []pb.HardState
}

func newSpaxosState() *spaxosState {
	return &spaxosState{chosen: make(map[uint64][]byte)}
}

func (ss *spaxosState) combine(ssb *spaxosState) {
	ss.rebuild = append(ss.rebuild, ssb.rebuild...)
	ss.hss = append(ss.hss, ssb.hss...)
	ss.msgs = append(ss.msgs, ssb.msgs...)
	ss.prevHSS = append(ss.prevHSS, ssb.prevHSS...)
	for index, value := range ssb.chosen {
		ss.chosen[index] = value
	}
}

type spaxos struct {
	// id of myself
	id     uint64
	groups map[uint64]bool

	// [minIndex, +) ins in allSps, if not, create a new one
	// (0, minIndex) ins in allSps, if not, re-build base ond disk hardstate
	// for index in (0, minIndex), will be a fifo queue
	minIndex uint64
	maxIndex uint64
	// pb.Message => index => ins need to re-build base on disk hardstate
	fifoIndex []uint64
	allSps    map[uint64]*spaxosInstance
	mySps     map[uint64]*spaxosInstance

	// msg handOn: wait for ins re-build;
	handOn map[uint64][]pb.Message

	prevState *spaxosState
	currState *spaxosState
	//	// index -> ins: need re-build
	//	rebuild []uint64 // rebuild index
	//	chosen  map[uint64][]byte
	//	hss     []pb.HardState
	//	msgs    []pb.Message
	//
	//	prevHSS []pb.HardState
}

func (sp *spaxos) submitChosen(ins *spaxosInstance) {
	assert(nil != ins)
	assert(true == ins.chosen)
	// TODO
	// submit ins.index, ins.proposingValue => chosen
}

func (sp *spaxos) reportChosen(index uint64, value []byte) {
	chosen := sp.currState.chosen
	if _, ok := chosen[index]; ok {
		log.Panic("reportChosen", index, "multiple-times!")
	}

	chosen[index] = value
	if _, ok := sp.mySps[index]; ok {
		delete(sp.mySps, index)
	}
}

func (sp *spaxos) appendMsg(msg pb.Message) {
	if 0 == msg.From {
		msg.From = sp.id
	}
	assert(msg.From == sp.id)
	assert(nil != sp.currState)
	sp.currState.msgs = append(sp.currState.msgs, msg)
}

func (sp *spaxos) appendHardState(hs pb.HardState) {
	sp.currState.hss = append(sp.currState.hss, hs)
}

func (sp *spaxos) appendHSS(hs pb.HardState) {
	sp.currState.hss = append(sp.currState.hss, hs)
}

func newSpaxos(
	selfid uint64, groupsid []uint64, minIndex, maxIndex uint64) *spaxos {
	groups := make(map[uint64]bool)
	for _, id := range groupsid {
		groups[id] = true
	}

	if _, ok := groups[selfid]; !ok {
		// ERROR CASE
		return nil
	}

	sp := &spaxos{
		id:        selfid,
		groups:    groups,
		minIndex:  minIndex,
		maxIndex:  maxIndex,
		allSps:    make(map[uint64]*spaxosInstance),
		mySps:     make(map[uint64]*spaxosInstance),
		handOn:    make(map[uint64][]pb.Message),
		currState: newSpaxosState()}
	//		chosen:   make(map[uint64][]byte)}
	assert(nil != sp)
	return sp
}

func (sp *spaxos) newSpaxosInstance() *spaxosInstance {
	// find the max idx num
	sp.maxIndex += 1
	ins := &spaxosInstance{
		index: sp.maxIndex, sp: sp,
		proposer: &roleProposer{}, acceptor: &roleAcceptor{}}
	_, ok := sp.allSps[sp.maxIndex]
	assert(false == ok)

	sp.allSps[sp.maxIndex] = ins
	sp.mySps[sp.maxIndex] = ins
	return ins
}

func (sp *spaxos) rebuildSpaxosInstance(hs pb.HardState) *spaxosInstance {
	ins := &spaxosInstance{
		index:    hs.Index,
		proposer: rebuildProposer(hs),
		acceptor: rebuildAcceptor(hs),
		hs:       hs,
		sp:       sp}
	return ins
}

func (sp *spaxos) getSpaxosInstance(msg pb.Message) *spaxosInstance {
	index := msg.Index
	ins, ok := sp.allSps[index]
	if !ok {
		if index < sp.minIndex {
			sp.fifoIndex = append(sp.fifoIndex, index)
			sp.handOn[index] = append(sp.handOn[index], msg)
			sp.currState.rebuild = append(sp.currState.rebuild, index)
			return nil
		}

		// create a new ins
		ins = &spaxosInstance{index: index, sp: sp,
			proposer: &roleProposer{}, acceptor: &roleAcceptor{}}
		sp.allSps[index] = ins
		sp.maxIndex = max(sp.maxIndex, index)
	}

	assert(nil != ins)
	return ins
}

func (sp *spaxos) needRebuild(index uint64) bool {
	if _, ok := sp.allSps[index]; ok {
		return false
	}

	if _, ok := sp.handOn[index]; !ok {
		return false
	}

	return true
}

func (sp *spaxos) Step(msg pb.Message) {
	switch msg.Type {
	case pb.MsgCliProp:
		var ins *spaxosInstance
		if 0 != msg.Index {
			// reprop at exist index
			ins = sp.getSpaxosInstance(msg)
			if nil == ins {
				return
			}
		} else {
			// 0 == msg.Index
			ins = sp.newSpaxosInstance()
			log.Printf("==> newSpaxosInstance index %d", ins.index)
		}

		assert(nil != ins)
		ins.proposer.propose(ins, msg.Entry.Value)
		return
	case pb.MsgInsRebuild:
		if !sp.needRebuild(msg.Index) {
			return // no need rebuld
		}

		newins := sp.rebuildSpaxosInstance(msg.Hs)
		sp.allSps[msg.Index] = newins

		msgs := sp.handOn[msg.Index]
		delete(sp.handOn, msg.Index)

		// apply
		for _, oldmsg := range msgs {
			newins.step(oldmsg)
		}
		return
	}

	// else =>
	ins := sp.getSpaxosInstance(msg)
	if nil == ins {
		// handOn: wait for ins rebuild
		print("wait to rebuild ins")
		return
	}

	if pb.MsgChosen == msg.Type {
		printMsg("msgChosen Test", msg)
		// chosen
		assert(msg.Index == ins.index)
		ins.reportChosen(msg.Entry.Value)
		return
	}

	ins.step(msg)
}

func (sp *spaxos) validNode(nodeID uint64) bool {
	if MaxNodeID <= nodeID {
		return false
	}

	_, ok := sp.groups[nodeID]
	return ok
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}

	return b
}
