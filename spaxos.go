package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type spaxosInstance struct {
	chosen   bool
	index    uint64
	proposer *roleProposer
	acceptor *roleAcceptor

	hs pb.HardState
	// ref
	sp *spaxos
}

func (ins *spaxosInstance) append(msg pb.Message) {
	ins.sp.msgs = append(ins.sp.msgs, msg)
}

func (ins *spaxosInstance) updatePropHardState(maxPropNum uint64) {
	hs := ins.hs
	assert(hs.MaxProposedNum < maxPropNum)
	hs.MaxProposedNum = maxPropNum
	ins.sp.hss = append(ins.sp.hss, hs)
}

func (ins *spaxosInstance) updateAccptHardState(
	maxPromisedNum, maxAcceptedNum uint64, acceptedValue []byte) {
	hs := ins.hs
	assert(hs.MaxPromisedNum <= maxPromisedNum)
	assert(hs.MaxAcceptedNum <= maxAcceptedNum)
	hs.MaxPromisedNum = maxPromisedNum
	hs.MaxAcceptedNum = maxAcceptedNum
	hs.AcceptedValue = acceptedValue

	sp := ins.sp
	sp.hss = append(sp.hss, hs)
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

	switch msg.Type {
	case pb.MsgProp:
		fallthrough
	case pb.MsgAccpt:
		return ins.acceptor.step(ins, msg)
	}

	return ins.proposer.step(ins, msg)
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

	// index -> ins: need re-build
	rebuild []uint64 // rebuild index
	chosen  map[uint64]pb.ProposeValue
	hss     []pb.HardState
	msgs    []pb.Message

	prevHSS []pb.HardState
}

func (sp *spaxos) newSpaxosInstance() *spaxosInstance {
	// find the max idx num
	sp.maxIndex += 1
	ins := &spaxosInstance{index: sp.maxIndex, sp: sp}
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
			sp.rebuild = append(sp.rebuild, index)
			return nil
		}

		// create a new ins
		ins = &spaxosInstance{index: index, sp: sp}
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
