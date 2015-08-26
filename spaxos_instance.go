package spaxos

import (
	pb "github.com/dengoswei/spaxos/spaxospb"
	// pb "spaxos/spaxospb"
)

type stepFunc func(sp *spaxos, msg pb.Message)

type spaxosInstance struct {
	chosen        bool
	index         uint64
	hostPropReqid uint64

	// proposer
	maxProposedNum     uint64
	maxAcceptedHintNum uint64
	proposingValue     *pb.ProposeItem
	rspVotes           map[uint64]bool
	tryNoopProp        bool
	noopVotes          map[uint64]bool

	// isPromised   bool
	stepProposer stepFunc

	// acceptor
	promisedNum   uint64
	acceptedNum   uint64
	acceptedValue *pb.ProposeItem

	// last active time stamp
	timeoutAt uint64
}

func newSpaxosInstance(index uint64) *spaxosInstance {
	ins := &spaxosInstance{index: index}
	ins.stepProposer = ins.stepPassiveTimeout
	return ins
}

func rebuildSpaxosInstance(hs pb.HardState) *spaxosInstance {
	ins := spaxosInstance{
		chosen:         hs.Chosen,
		index:          hs.Index,
		hostPropReqid:  hs.HostPropReqid,
		maxProposedNum: hs.MaxProposedNum,
		promisedNum:    hs.MaxPromisedNum,
		acceptedNum:    hs.MaxAcceptedNum,
		acceptedValue:  hs.AcceptedValue}
	ins.stepProposer = ins.stepPassiveTimeout
	return &ins
}

func (ins *spaxosInstance) getHardState() pb.HardState {
	return pb.HardState{
		Chosen:         ins.chosen,
		Index:          ins.index,
		HostPropReqid:  ins.hostPropReqid,
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

	// not nil
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

	// assert(nil != msg.Entry.Value)
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
func (ins *spaxosInstance) Propose(
	sp *spaxos, proposingValue *pb.ProposeItem, asMaster bool) {
	assert(nil != sp)
	assert(false == ins.chosen)

	assert(nil == ins.proposingValue)
	assert(0 == ins.hostPropReqid)
	// never proposing nil value
	// => no-op propose don't use this function
	assert(nil != proposingValue)
	assert(0 != proposingValue.Reqid)
	ins.proposingValue = proposingValue
	ins.hostPropReqid = proposingValue.Reqid
	//if false == ins.chosen {
	//	ins.proposingValue = proposingValue
	//}

	// TODO: master propose: skip prepare phase
	ins.tryNoopProp = false
	ins.beginPreparePhase(sp, false)
}

// TODO: no-op propose
func (ins *spaxosInstance) NoopPropose(sp *spaxos) {
	assert(nil != sp)
	assert(false == ins.chosen)

	ins.tryNoopProp = true
	ins.proposingValue = nil
	ins.beginPreparePhase(sp, false)
}

// end of no-op propose

func (ins *spaxosInstance) beginPreparePhase(sp *spaxos, dropReq bool) {
	assert(nil != sp)
	if ins.chosen {
		ins.markChosen(sp, false)
		return
	}

	// ins.isPromised = false
	// inc ins.maxProposedNum
	nextProposeNum := sp.getNextProposeNum(
		ins.maxProposedNum, ins.promisedNum)

	req := pb.Message{
		Type: pb.MsgProp, Index: ins.index,
		From: sp.id, Entry: pb.PaxosEntry{PropNum: nextProposeNum}}

	// optimize: local check first
	ins.rspVotes = make(map[uint64]bool)
	if !ins.tryNoopProp {
		rsp := ins.updatePromised(req)
		assert(false == rsp.Reject)
		assert(0 == rsp.From)
		// step promised: update
		if ins.maxAcceptedHintNum < rsp.Entry.AccptNum {
			LogDebug("%s update maxAcceptedHintNum %d -> %d",
				GetFunctionName(ins.beginPreparePhase),
				ins.maxAcceptedHintNum, rsp.Entry.AccptNum)
			ins.maxAcceptedHintNum = rsp.Entry.AccptNum
			ins.proposingValue = rsp.Entry.Value
		}

		ins.rspVotes[sp.id] = true
	} else {
		// tryNoopProp
		ins.noopVotes = make(map[uint64]bool)
	}

	ins.maxProposedNum = nextProposeNum
	ins.stepProposer = ins.stepPrepareRsp

	if !dropReq {
		sp.appendMsg(req)
	}
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) beginAcceptPhase(sp *spaxos, dropReq bool) {
	assert(nil != sp)
	if ins.chosen {
		ins.markChosen(sp, false)
		return
	}

	// ins.isPromised = true
	req := pb.Message{
		Type:  pb.MsgAccpt,
		Index: ins.index, From: sp.id,
		Entry: pb.PaxosEntry{
			PropNum: ins.maxProposedNum, Value: ins.proposingValue}}

	// optimize: local check first
	{
		rsp := ins.updateAccepted(req)
		if true == rsp.Reject {
			// reject by self
			// => backoff to beginPreparePhase
			// NOTE: this may cause live lock
			ins.beginPreparePhase(sp, false)
			return
		}

		assert(false == rsp.Reject)
		assert(0 == rsp.From)
	}

	ins.rspVotes = make(map[uint64]bool)
	ins.rspVotes[sp.id] = true
	ins.stepProposer = ins.stepAcceptRsp

	if !dropReq {
		sp.appendMsg(req)
	}
	sp.appendHardState(ins.getHardState())
}

func (ins *spaxosInstance) markChosen(sp *spaxos, broadcast bool) {
	assert(nil != sp)

	ins.chosen = true
	ins.rspVotes = nil
	sp.submitChosen(ins.index)
	ins.stepProposer = nil
	if broadcast {
		req := pb.Message{
			Type:  pb.MsgChosen,
			Index: ins.index, From: sp.id,
			Entry: pb.PaxosEntry{Value: ins.acceptedValue}}
		sp.appendMsg(req)
	}
}

func (ins *spaxosInstance) stepPrepareRsp(
	sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	if pb.MsgTimeOut == msg.Type {
		// timeout happen: act as if reject by major ?
		ins.beginPreparePhase(sp, false)
		return
	}

	// only deal with MsgPropResp msg;
	// println("=>", pb.MsgPropResp, msg.Type, ins.maxProposedNum, msg.Entry.PropNum)
	if pb.MsgPropResp != msg.Type ||
		ins.maxProposedNum != msg.Entry.PropNum {
		return // ignore the mismatch prop num msg
	}

	//	println("==>", msg.From, msg.To, len(ins.rspVotes))
	if val, ok := ins.rspVotes[msg.From]; ok {
		// inconsist !
		assert(val == !msg.Reject)
		return
	}

	ins.rspVotes[msg.From] = !msg.Reject
	if false == msg.Reject {
		// update the maxAcceptedHitNum & proposingValue
		if ins.maxAcceptedHintNum < msg.Entry.AccptNum {
			ins.maxAcceptedHintNum = msg.Entry.AccptNum
			ins.proposingValue = msg.Entry.Value
		}

		if ins.tryNoopProp {
			ins.noopVotes[msg.From] = nil == msg.Entry.Value
		}
	}

	if rejectedByMajority(sp, ins.rspVotes) {
		ins.beginPreparePhase(sp, false)
	} else {
		if ins.tryNoopProp {
			if noopByMajority(sp, ins.noopVotes) {
				// Since noop by Majority it's safe to
				// ignore <maxAcceptedHitNum, proposingValue>,
				// by using nil as proposingValue instread;
				ins.maxAcceptedHintNum = 0
				ins.proposingValue = nil
				ins.beginAcceptPhase(sp, false)
			} else if sp.recvAllRsp(ins.rspVotes) {
				// don't reach noopByMajority after recv all resp
				// => backoff to normal prop
				ins.tryNoopProp = false // reset
				ins.beginPreparePhase(sp, false)
			}
		} else if promisedByMajority(sp, ins.rspVotes) {
			ins.beginAcceptPhase(sp, false)
		}
	}
}

func (ins *spaxosInstance) stepAcceptRsp(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	if pb.MsgTimeOut == msg.Type {
		// timeout happen: redo beginAccepted ?
		ins.beginAcceptPhase(sp, false)
		return
	}

	// TODO
	// only deal with MsgAccptResp msg;
	if pb.MsgAccptResp != msg.Type ||
		ins.maxProposedNum != msg.Entry.PropNum {
		return // ignore the mismatch prop num msg
	}

	if val, ok := ins.rspVotes[msg.From]; ok {
		// inconsist !
		assert(val == !msg.Reject)
		return
	}

	ins.rspVotes[msg.From] = !msg.Reject
	if acceptedByMajority(sp, ins.rspVotes) {
		ins.markChosen(sp, true)
	} else if rejectedByMajority(sp, ins.rspVotes) {
		ins.beginPreparePhase(sp, false)
	}
}

func (ins *spaxosInstance) stepPassiveTimeout(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)

	assert(false == ins.chosen)
	assert(0 == ins.hostPropReqid)
	assert(nil == ins.proposingValue)
	assert(0 == ins.maxProposedNum)
	// TODO
	LogDebug("%s hostid %d msg %v", GetCurrentFuncName(), sp.id, msg)
}

// TODO: add stepChosen test-case
func (ins *spaxosInstance) stepChosen(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(ins.index == msg.Index)
	assert(true == ins.chosen)
	switch msg.Type {
	case pb.MsgProp, pb.MsgAccpt:
		// all msg will be ignore
		chosenMsg := pb.Message{
			Type:  pb.MsgChosen,
			Index: ins.index, From: sp.id, To: msg.From,
			Entry: pb.PaxosEntry{Value: ins.acceptedValue}}
		sp.appendMsg(chosenMsg)
	default:
		// ignore ?
		LogDebug("%s msg %v", GetFunctionName(ins.stepChosen), msg)
	}
}

func (ins *spaxosInstance) stepTryCatchUp(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(msg.Index == ins.index)
	assert(false == ins.chosen)

	// broadcast msg
	// => spaxos_instance don't explicit deal with MsgCatchUp,
	// only chosen spaxos_instance will response with MsgChosen;
	catchReq := pb.Message{
		Type:  pb.MsgCatchUp,
		Index: ins.index, From: sp.id, To: 0}
	sp.appendMsg(catchReq)
}

func (ins *spaxosInstance) step(sp *spaxos, msg pb.Message) {
	assert(nil != sp)
	assert(sp.id == msg.To)
	assert(0 != msg.Index)
	if pb.MsgTimeOut == msg.Type &&
		ins.timeoutAt > msg.Timestamp {
		// timeout isn't valid anymore
		return
	}

	if ins.chosen {
		ins.stepChosen(sp, msg)
		return
	}

	switch msg.Type {
	case pb.MsgTryCatchUp:
		ins.stepTryCatchUp(sp, msg)

	case pb.MsgChosen:
		ins.beginPreparePhase(sp, true)
		ins.maxAcceptedHintNum = 0
		ins.proposingValue = msg.Entry.Value
		ins.beginAcceptPhase(sp, true)
		assert(ins.acceptedNum == ins.promisedNum)
		assert(ins.acceptedNum == ins.maxProposedNum)
		assert(true == ins.acceptedValue.Equal(msg.Entry.Value))
		ins.markChosen(sp, false)

	case pb.MsgProp, pb.MsgAccpt:
		// step Acceptor don't need to deal with timeout msg
		ins.stepAcceptor(sp, msg)

	case pb.MsgPropResp, pb.MsgAccptResp, pb.MsgTimeOut:
		assert(nil != ins.stepProposer)
		ins.stepProposer(sp, msg)
	default:
		LogDebug("%s ignore msg %v", GetFunctionName(ins.step), msg)
		return
	}

	prevTimeout := ins.timeoutAt
	sp.updateTimeout(ins)
	// may update
	assert(prevTimeout <= ins.timeoutAt)
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

func noopByMajority(sp *spaxos, noopVotes map[uint64]bool) bool {
	assert(nil != sp)
	return sp.asMajority(noopVotes, true)
}
