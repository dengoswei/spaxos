package spaxos

import (
	pb "spaxos/spaxospb"
)

type stepFunc func(sp *spaxos, msg pb.Message)

type spaxosInstance struct {
	chosen bool
	index  uint64
	// proposer
	maxProposedNum     uint64
	maxAcceptedHintNum uint64
	proposingValue     []byte
	rspVotes           map[uint64]bool
	stepProposer       stepFunc

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
	case pb.MsgProp:
		rsp = ins.updatePromised(msg)
	case pb.MsgAccpt:
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
		ins.markChosen(sp, false)
		return
	}

	// inc ins.maxProposedNum
	nextProposeNum := sp.getNextProposeNum(
		ins.maxProposedNum, ins.promisedNum)

	req := pb.Message{
		Type: pb.MsgProp, Index: ins.index, From: sp.id,
		Entry: pb.PaxosEntry{PropNum: nextProposeNum}}

	// optimize: local check first
	{
		rsp := ins.updatePromised(req)
		assert(false == rsp.Reject)
		assert(0 == rsp.From)
	}

	ins.rspVotes = make(map[uint64]bool)
	ins.rspVotes[sp.id] = true
	ins.maxProposedNum = nextProposeNum
	ins.stepProposer = ins.stepPrepareRsp

	sp.appendMsg(req)
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) beginAcceptPhase(sp *spaxos) {
	assert(nil != sp)
	if ins.chosen {
		ins.markChosen(sp, false)
		return
	}

	req := pb.Message{
		Type: pb.MsgAccpt, Index: ins.index, From: sp.id,
		Entry: pb.PaxosEntry{
			PropNum: ins.maxProposedNum, Value: ins.proposingValue}}

	// optimize: local check first
	{
		rsp := ins.updateAccepted(req)
		if true == rsp.Reject {
			// reject by self
			// => backoff to beginPreparePhase
			// NOTE: this may cause live lock
			ins.beginPreparePhase(sp)
			return
		}

		assert(false == rsp.Reject)
		assert(0 == rsp.From)
	}

	ins.rspVotes = make(map[uint64]bool)
	ins.rspVotes[sp.id] = true
	ins.stepProposer = ins.stepAcceptRsp

	sp.appendMsg(req)
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) markChosen(sp *spaxos, broadcast bool) {
	assert(nil != sp)

	ins.chosen = true
	sp.submitChosen(ins)
	ins.stepProposer = ins.stepChosen
	if broadcast {
		req := pb.Message{
			Type: pb.MsgChosen, Index: ins.index, From: sp.id,
			Entry: pb.PaxosEntry{Value: ins.acceptedValue}}
		sp.appendMsg(req)
	}
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
		ins.markChosen(sp, true)
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