package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

type stepFunc func(ins *spaxosInstance, msg *pb.Message) (bool, error)

type roleProposer struct {
	// wait
	// => { prepare -> accpet} loop
	step stepFunc

	//
	proposingRound uint32
	maxProposedNum uint64
	maxPromisedNum uint64
	proposingValue []byte
	votes          map[uint64]bool
}

func (p *roleProposer) propose(ins *spaxosInstance, value []byte) bool {
	p.proposingRound = 0
	p.maxProposedNum = 0
	p.maxPromisedNum = 0
	// default proposing value:
	p.proposingValue = value

	// gen prepare msg & hard state
	p.beginPrepare(ins)
	p.step = stepPrepare
	return true
}

func (p *roleProposer) beginPrepare(ins *spaxosInstance) {
	p.proposingRound += 1
	p.maxProposedNum = ins.nextProposeNum(p.maxProposedNum)
	p.votes = make(map[uint64]bool)

	// index
	hs := &pb.HardState{Type: pb.HardStateProp,
		Index: ins.index, MaxProposedNum: p.maxProposedNum}

	msg := &pb.Message{Type: pb.MsgProp,
		Index: ins.index, Entry: pb.PaxosEntry{PropNum: p.maxProposedNum}}

	ins.hss.append(hs)
	ins.msgs.append(msg)
}

func (p *roleProposer) beginAccept(ins *spaxosInstance) {
	p.votes = make(map[uint64]bool)

	msg := &pb.Message{Type: pb.MsgAccpt,
		Index: ins.index, Entry: pb.PaxosEntry{
			PropNum: p.maxProposedNum, Value: p.propsingValue}}

	ins.msgs.append(msg)
}

func (p *roleProposer) stepPrepare(ins *spaxosInstance, msg *pb.Message) (bool, error) {
	// MsgPropResp or MsgMajorReject
	if msg.Index != ins.index || p.maxProposedNum != msg.Entry.PropNum {
		return true, nil // do nothing: error PropNum
	}

	switch msg.Type {
	case pb.MsgPropResp:
	case pb.MsgTimeOut:
		fallthrough
	case pb.MsgMajorReject:
		// redo
		p.beginPrepare(ins)
		return true, nil
	default:
		return false, error.New("proposer: error msg type")
	}

	if val, ok := p.votes[msg.from]; ok {
		// msg.from already vote
		assert(val == !msg.Reject)
		return true, nil
	}

	p.votes[msg.from] = !msg.Reject
	if false == msg.Reject {
		if p.maxPromisedNum < msg.Entry.AccptNum {
			p.proposingValue = msg.Entry.Value
		}

		// if maxPromisedNum == msg.Entry.AccptNum
		// => p.proprosingValue == msg.Entry.Value
	}

	if p.trueByMajority(ins) {
		p.beginAccept(ins)
		p.step = stepAccept
	} else if proposer.falseByMajority(sp) {
		p.beginPrepare(ins)
		return true, nil
	}

	return true, nil
}

func (p *roleProposer) stepAccept(ins *spaxosInstance, msg *pb.Message) (bool, error) {
	if msg.Index != ins.index || p.maxProposedNum != msg.Entry.PropNum {
		return true, nil
	}

	switch msg.Type {
	case pb.MsgAccptResp:
	case pb.MsgTimeOut:
		fallthrough
	case pb.MsgMajorReject:
		// redo
		p.beginAccept(ins)
		return true, nil
	default:
		return false, error.New("proposer: error msg type")
	}

	if val, ok := p.votes[msg.from]; ok {
		assert(val == !msg.Reject)
		return true, nil
	}

	p.votes[msg.from] = !msg.Reject
	if ins.trueByMajority(p.votes) {
		// accpeted by majority
		p.step = stepChosen
		ins.chosen = true
		return true, nil
	} else if ins.falseByMajority(p.votes) {
		// reject by majority
		p.beginPrepare(ins)
		p.step = stepPrepare
		return true, nil
	}

	return true, nil
}
