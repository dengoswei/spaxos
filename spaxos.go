package spaxos

import (
	"errors"
	"log"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type spaxosInstance struct {
	chosen bool
	index  uint64
	// proposer
	maxProposedNum uint64
	maxPromisedNum uint64
	proposingValue []byte
	pRspVotes      map[uint64]bool
	aRspVotes      map[uint64]bool

	// acceptor
	promisedNum   uint64
	acceptedNum   uint64
	acceptedValue []byte
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

func (ins *spaxosInstance) reportChosen(value []byte) {
	ins.sp.reportChosen(ins.index, value)
}

func (ins *spaxosInstance) append(msg pb.Message) {
	assert(nil != ins.sp)
	ins.sp.appendMsg(msg)
}

func (ins *spaxosInstance) updatePropHardState(maxPropNum uint64) {
	hs := ins.hs
	assert(hs.MaxProposedNum < maxPropNum)
	hs.MaxProposedNum = maxPropNum
	ins.sp.appendHSS(hs)
}

func (ins *spaxosInstance) updateAccptHardState(
	maxPromisedNum, maxAcceptedNum uint64, acceptedValue []byte) {
	hs := ins.hs
	assert(hs.MaxPromisedNum <= maxPromisedNum)
	assert(hs.MaxAcceptedNum <= maxAcceptedNum)
	hs.MaxPromisedNum = maxPromisedNum
	hs.MaxAcceptedNum = maxAcceptedNum
	hs.AcceptedValue = acceptedValue

	ins.sp.appendHSS(hs)
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

func (ins *spaxosInstance) nextProposeNum(propNum uint64) uint64 {
	if 0 == propNum {
		return ins.sp.id
	}

	return propNum + MaxNodeID
}

func (ins *spaxosInstance) step(msg pb.Message) (bool, error) {
	if msg.Index != ins.index {
		return false, errors.New("spaxos: mismatch index")
	}

	printMsg("spaxosInstance step", msg)
	switch msg.Type {
	case pb.MsgProp:
		fallthrough
	case pb.MsgAccpt:
		return ins.acceptor.step(ins, msg)
	case pb.MsgPropResp:
		fallthrough
	case pb.MsgAccptResp:
		return ins.proposer.step(ins, msg)
	}

	log.Fatal("msg invalid step type %s",
		pb.MessageType_name[int32(msg.Type)])
	return false, errors.New("spaxos: invalid step msg.Type")
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
