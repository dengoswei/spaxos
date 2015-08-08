package spaxos

import (
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
	stepProposer       stepSpaxosFunc

	// acceptor
	promisedNum   uint64
	acceptedNum   uint64
	acceptedValue []byte
	// stepAccpt     stepSpaxosFunc
}

func newSpaxosInstance(index uint64) *spaxosInstance {
	return &spaxosInstance{index: index}
}

func rebuildSpaxosInstance(hs pb.HardState) *spaxosInstance {
	ins := spaxosInstance{
		chosen: hs.Chosen, index: hs.Index,
		maxProposedNum: hs.MaxProposedNum,
		promisedNum:    hs.MaxPromisedNum,
		acceptedNum:    hs.MaxAcceptedNum,
		acceptedValue:  hs.AcceptedValue}
	return &ins
}

func (ins *spaxosInstance) getHardState() pb.HardState {
	return pb.HardState{
		Chosen:         ins.chosen,
		Index:          ins.index,
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

// acceptor
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

func (ins *spaxosInstance) stepAcceptor(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)

	var rsp pb.Message
	switch msg.Type {
	case pb.MsgPropResp:
		rsp = ins.updatePromised(msg)
	case pb.MsgAccptResp:
		rsp = ins.updateAccepted(msg)
	default:
		assert(false)
	}

	sp.appendMsg(rsp)
	if false == rsp.Reject {
		sp.appendHardState(ins.getHardState())
	}
}

// proposer
func (ins *spaxosInstance) Propose(sp *spaxos, proposingValue []byte) {
	assert(nil != sp)
	assert(nil != proposingValue)
	if false == ins.chosen {
		ins.proposingValue = proposingValue
	}

	ins.beginPreparePhase(sp)
}

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
	ins.stepProposer = ins.stepPrepareRsp

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
	ins.stepProposer = ins.stepAcceptRsp

	sp.appendMsg(req)
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) markChosen(sp *spaxos) {
	assert(nil != sp)

	ins.chosen = true
	sp.submitChosen(ins)
	ins.stepProposer = ins.stepChosen
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

	if promisedByMajority(sp, ins.rspVotes) {
		ins.beginAcceptPhase(sp)
	} else if rejectedByMajority(sp, ins.rspVotes) {
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
	if acceptedByMajority(sp, ins.rspVotes) {
		ins.markChosen(sp)
	} else if rejectedByMajority(sp, ins.rspVotes) {
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

func (sp *spaxos) getNextProposeNum(prev, hint uint64) uint64 {
	assert(0 != sp.id)
	hint = MaxUint64(prev, hint)
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

func promisedByMajority(sp *spaxos, rspVotes map[uint64]bool) bool {
	assert(nil != sp)
	return sp.asMajority(rspVotes, true)
}

func acceptedByMajority(sp *spaxos, rspVotes map[uint64]bool) bool {
	assert(nil != sp)
	return sp.asMajority(rspVotes, true)
}

func rejectedByMajority(sp *spaxos, rspVotes map[uint64]bool) bool {
	assert(nil != sp)
	return sp.asMajority(rspVotes, false)
}
