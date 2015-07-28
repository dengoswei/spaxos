package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

type spaxosEntry struct {
	index    uint64
	proposer roleProposer
	acceptor roleAcceptor
}

const MaxNodeID uint64 = 1024

type spaxos struct {
	// id of myself
	id uint64

	groups map[uint64]bool

	currentIndex uint64
	currentEntry spaxosEntry
	// groups of node
	groups map[uint64]bool
}

func (sp *spaxos) validNode(nodeID uint64) bool {
	if MaxNodeID <= nodeID {
		return false
	}

	val, ok := sp.groups[nodeID]
	return ok
}

func (sp *spaos) nextProposeNum(proposeNum uint64) uint64 {
	return proposeNum + MaxNodeID
}
