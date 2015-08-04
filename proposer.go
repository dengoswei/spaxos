package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

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

func rebuildProposer(hs pb.HardState) *roleProposer {
	p := &roleProposer{maxProposedNum: hs.MaxProposedNum}
	return p
}

func (p *roleProposer) propose(ins *spaxosInstance, value []byte) {
	p.proposingRound = 0
	p.maxPromisedNum = 0
	// default proposing value:
	p.proposingValue = value
	p.votes = make(map[uint64]bool)

	// gen prepare msg & hard state
	p.beginPrepare(ins)
	p.step = p.stepPrepare
}

func (p *roleProposer) beginPrepare(ins *spaxosInstance) {
	p.proposingRound += 1
	p.maxProposedNum = ins.nextProposeNum(p.maxProposedNum)
	p.votes = make(map[uint64]bool)

	// index
	msg := pb.Message{Type: pb.MsgProp,
		Index: ins.index, Entry: pb.PaxosEntry{PropNum: p.maxProposedNum}}

	ins.updatePropHardState(p.maxProposedNum)
	ins.append(msg)
}

func (p *roleProposer) beginAccept(ins *spaxosInstance) {
	p.votes = make(map[uint64]bool)

	msg := pb.Message{Type: pb.MsgAccpt,
		Index: ins.index, Entry: pb.PaxosEntry{
			PropNum: p.maxProposedNum, Value: p.proposingValue}}

	ins.append(msg)
}

type stepFunc func(ins *spaxosInstance, msg pb.Message) (bool, error)

func (p *roleProposer) stepChosen(
	sp *spaxosInstance, msg pb.Message) (bool, error) {
	return true, nil
}

func (p *roleProposer) stepPrepare(
	ins *spaxosInstance, msg pb.Message) (bool, error) {
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
		return false, errors.New("proposer: error msg type")
	}

	if val, ok := p.votes[msg.From]; ok {
		// msg.from already vote
		assert(val == !msg.Reject)
		return true, nil
	}

	p.votes[msg.From] = !msg.Reject
	if false == msg.Reject {
		if p.maxPromisedNum < msg.Entry.AccptNum {
			p.proposingValue = msg.Entry.Value
		}

		// if maxPromisedNum == msg.Entry.AccptNum
		// => p.proprosingValue == msg.Entry.Value
	}

	if ins.trueByMajority(p.votes) {
		p.beginAccept(ins)
		p.step = p.stepAccept
	} else if ins.falseByMajority(p.votes) {
		p.beginPrepare(ins)
		return true, nil
	}

	return true, nil
}

func (p *roleProposer) stepAccept(
	ins *spaxosInstance, msg pb.Message) (bool, error) {
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
		return false, errors.New("proposer: error msg type")
	}

	if val, ok := p.votes[msg.From]; ok {
		assert(val == !msg.Reject)
		return true, nil
	}

	p.votes[msg.From] = !msg.Reject
	if ins.trueByMajority(p.votes) {
		// accpeted by majority
		p.step = p.stepChosen
		ins.reportChosen(p.proposingValue)
		return true, nil
	} else if ins.falseByMajority(p.votes) {
		// reject by majority
		p.beginPrepare(ins)
		p.step = p.stepPrepare
		return true, nil
	}

	return true, nil
}
