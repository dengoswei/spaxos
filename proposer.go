package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

type ProposerStatus int
type stepProposeFunc func(sp *spaxos, msgs []pb.Message) (bool, error)

const (
	ProposerNil     ProposerStatus = 0
	ProposerPrepare ProposerStatus = 1
	ProposerAccept  ProposerStatus = 2
	ProposerChosen  ProposerStatus = 3
)

type roleProposer struct {
	// wait
	// => { prepare -> accpet} loop
	status ProposerStatus
	step   stepProposeFunc

	//
	proposingRound uint32
	maxProposeNum  uint64
	maxPromisedNum uint64
	proposingValue []byte
	votes          map[uint64]bool
}

func (proposer *roleProposer) propose(value []byte) error {
	if ProposerNil != proposer.status {
		return errors.New("spaxos: re-propose")
	}

	proposer.proposingRound = 0
	proposer.maxProposeNum = 0
	proposer.maxPromisedNum = 0
	// default proposing value:
	proposer.proposingValue = value
	proposer.ProposerStatus = ProposerPrepare
	proposer.step = stepInPrepare
	return nil
}

func (proposer *roleProposer) getMessages() (State, error) {
	switch proposer.status {
	case ProposerPrepare:
		proposer.proposingRound += 1
		proposer.maxPromisedNum = sp.nextProposeNum(proposer.maxProposeNum)
		proposer.votes = make(map[uint64]bool)
		return State{
			msg: &pb.Message{
				Type: pb.MsgProp,
				Entry: pb.PaxosEntry{
					PropNum: proposer.maxProposeNum}}}, nil
	case ProposerAccept:
		proposer.votes = make(map[uint64]bool)
		return State{
			state: &pb.HardState{
				maxProposeNum: proposer.maxProposeNum},
			msg: &pb.Message{
				Type: pb.MsgAccpt,
				Entry: pb.PaxosEntry{
					PropNum: proposer.maxProposeNum,
					Value:   proposer.proposingValue}}}, nil
	default:
		return State{}, error.New("spaxos: unexpected status")
	}
	assert(0)
}

func (proposer *roleProposer) stepInPrepare(sp *spaxos, msgs []pb.Message) (bool, error) {
	assert(ProposerPrepare == proposer.status)
	for idx, msg := range msgs {
		if pb.MsgPropResp != msg.Type {
			continue
		}

		if msg.Entry.PropNum != proposer.maxProposeNum {
			continue
		}

		assert(true == sp.validNode(msg.from))
		proposer.votes[msg.from] = !msg.Reject
		if false == msg.Reject {
			// promised
			// => try to select the maximum promised value as proposing value
			if proposer.maxPromisedNum < msg.Entry.AccptNum {
				proposer.proposingValue = msg.Entry.Value
			}
		}
	}

	// => if majority: reject or promised
	if proposer.promisedByMajority(sp) {
		proposer.status = ProposerAccept
		proposer.step = stepInAccept
		return true, nil
	} else if proposer.rejectByMajority(sp) {
		// status still ProposerPrepare
		return true, nil
	}

	// haven't reach majority yet
	return false, nil
}

func (proposer *roleProposer) stepInAccept(sp *spaxos, msgs []pb.Message) (bool, error) {
	assert(ProposerAccept == proposer.status)
	for idx, msg := range msgs {
		if pb.MsgAccptResp != msg.Type {
			continue
		}

		if msg.Entry.PropNum != proposer.maxProposeNum {
			continue
		}

		asserrt(true == sp.validNode(msg.from))
		proposer.votes[msg.from] = !msg.Reject
	}

	if proposer.acceptedByMajority(sp) {
		proposer.status = ProposerChosen
		proposer.step = nil
		return true, nil
	} else if proposer.rejectByMajority(sp) {
		// fall back into ProposerPrepare
		proposer.status = ProposerPrepare
		proposer.step = stepInPrepare
		return true, nil
	}

	// haven't reach majority yet
	return false, nil
}

func (proposer *roleProposer) feedMessages(sp *spaxos, msgs []pb.Message) (bool, error) {
	if nil == proposer.step {
		return true, error.New("spaxos: nil proposer step")
	}

	return proposer.step(sp, msgs)
}
