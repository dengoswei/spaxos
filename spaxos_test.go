package spaxos

import (
	"bytes"
	"math/rand"
	"testing"

	// pb "spaxos/spaxospb"
)

func TestnewSpaxosInstance(t *testing.T) {
	index := uint64(rand.Uint32())
	ins := newSpaxosInstance(index)
	assert(nil != ins)
	assert(index == ins.index)
	assert(false == ins.chosen)
	assert(0 == ins.maxProposedNum)
	assert(0 == ins.maxAcceptedHintNum)
	assert(nil == ins.proposingValue)
	assert(nil == ins.rspVotes)
	assert(nil == ins.stepProposer)
	assert(0 == ins.promisedNum)
	assert(0 == ins.acceptedNum)
	assert(nil == ins.acceptedValue)
}

func TestgetHardState(t *testing.T) {
	ins := RandSpaxosInstance()
	assert(nil != ins)

	hs := ins.getHardState()
	assert(hs.Chosen == ins.chosen)
	assert(hs.Index == ins.index)
	assert(hs.MaxProposedNum == ins.maxProposedNum)
	assert(hs.MaxPromisedNum == ins.promisedNum)
	assert(hs.MaxAcceptedNum == ins.acceptedNum)
	assert(0 == bytes.Compare(hs.AcceptedValue, ins.acceptedValue))
}

func TestrebuildSpaxosInstance(t *testing.T) {
	ins := RandSpaxosInstance()
	assert(nil != ins)

	hs := ins.getHardState()

	newins := rebuildSpaxosInstance(hs)
	assert(nil != newins)
	assert(ins.index == newins.index)
	assert(ins.maxProposedNum == newins.maxProposedNum)
	assert(ins.promisedNum == newins.promisedNum)
	assert(ins.acceptedNum == newins.acceptedNum)
	assert(0 == bytes.Compare(ins.acceptedValue, newins.acceptedValue))
}
