package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

type spaxosInstance struct {
	index    uint64
	proposer *roleProposer
	acceptor *roleAcceptor
}

func (ins *spaxosInstance) step(
	msg *pb.Message) (*pb.HardState, *pb.Message, error) {

	assert(msg.Index == ins.index)
	switch msg.Type {
	case pb.MsgPropResp:
		fallthrough
	case pb.MsgAccptResp:
		// prop
		assert(nil != ins.proposer)
		terminate, ecode := ins.proposer.feed(msg)
		if nil != ecode {
			return nil, nil, ecode
		}

		if terminate {
			// check status
			if ProposerChosen != ins.proposer.status {
				// not yet complete
				return ins.proposer.getMessage()
			}
		}

		// else
		return nil, nil, nil
	case pb.MsgProp:
		fallthrough
	case pb.MsgAccpt:
		// accpt
		assert(nil != ins.acceptor)
		return ins.acceptor.step(msg)
	}

	return nil, nil, error.New("spaxos: err msg type")
}

const MaxNodeID uint64 = 1024

type spaxos struct {
	// id of myself
	id uint64

	currentIndex uint64
	sps          map[uint64]spaxosInstance
	// groups of node
	groups map[uint64]bool

	progressLog []HardState
}

func (sp *spaxos) validNode(nodeID uint64) bool {
	if MaxNodeID <= nodeID {
		return false
	}

	val, ok := sp.groups[nodeID]
	return ok
}

func (sp *spaxos) nextProposeNum(proposeNum uint64) uint64 {
	return proposeNum + MaxNodeID
}

func (sp *spaxos) trueByMajority(votes map[uint64]bool) bool {
	total := len(groups)
	trueCnt := 0
	for idx, b := range votes {
		if b {
			trueCnt += 1
		}
	}

	return trueCnt > total/2
}

func (sp *spaxos) falseByMajority(votes map[uint64]bool) bool {
	total := len(groups)
	falseCnt := 0
	for idx, b := range votes {
		if !b {
			falseCnt += 1
		}
	}

	return falseCnt > total/2
}
