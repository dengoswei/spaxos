package spaxos

import (
	"bytes"
	"math/rand"
	"testing"

	pb "spaxos/spaxospb"
)

func TestNewSpaxosInstance(t *testing.T) {
	printIndicate()

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

func TestGetHardState(t *testing.T) {
	printIndicate()

	ins := randSpaxosInstance()
	assert(nil != ins)

	hs := ins.getHardState()
	assert(hs.Chosen == ins.chosen)
	assert(hs.Index == ins.index)
	assert(hs.MaxProposedNum == ins.maxProposedNum)
	assert(hs.MaxPromisedNum == ins.promisedNum)
	assert(hs.MaxAcceptedNum == ins.acceptedNum)
	assert(0 == bytes.Compare(hs.AcceptedValue, ins.acceptedValue))
}

func TestRebuildSpaxosInstance(t *testing.T) {
	printIndicate()

	ins := randSpaxosInstance()
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

// test accepted
func TestUpdatePromised(t *testing.T) {
	printIndicate()

	ins := randSpaxosInstance()

	msg := pb.Message{
		Type: pb.MsgProp, To: 1, From: 2,
		Index: ins.index,
		Entry: pb.PaxosEntry{PropNum: ins.maxProposedNum}}

	// case 1: reject
	ins.promisedNum = ins.maxProposedNum + 1
	rejectRsp := ins.updatePromised(msg)
	assert(pb.MsgPropResp == rejectRsp.Type)
	assert(rejectRsp.Index == ins.index)
	assert(rejectRsp.From == msg.To)
	assert(rejectRsp.To == msg.From)
	assert(true == rejectRsp.Reject)
	assert(rejectRsp.Entry.PropNum == msg.Entry.PropNum)

	// case 2: promised with accepted value
	ins.promisedNum = ins.maxProposedNum - 1
	assert(nil != ins.acceptedValue)
	rsp := ins.updatePromised(msg)
	assert(pb.MsgPropResp == rsp.Type)
	assert(rsp.Index == ins.index)
	assert(false == rsp.Reject)
	assert(rsp.Entry.PropNum == msg.Entry.PropNum)
	assert(rsp.Entry.AccptNum == ins.acceptedNum)
	assert(0 == bytes.Compare(rsp.Entry.Value, ins.acceptedValue))
	assert(ins.promisedNum == msg.Entry.PropNum)

	// case 3: promised with nil value
	ins.promisedNum = ins.maxProposedNum - 1
	ins.acceptedValue = nil
	ins.acceptedNum = 0
	rsp = ins.updatePromised(msg)
	assert(rsp.Index == ins.index)
	assert(false == rsp.Reject)
	assert(rsp.Entry.PropNum == msg.Entry.PropNum)
	assert(0 == rsp.Entry.AccptNum)
	assert(nil == rsp.Entry.Value)
	assert(ins.promisedNum == msg.Entry.PropNum)
}

func TestUpdateAccepted(t *testing.T) {
	printIndicate()

	ins := randSpaxosInstance()

	propValue := RandByte(100)

	msg := pb.Message{
		Type: pb.MsgAccpt, To: 1, From: 2,
		Index: ins.index,
		Entry: pb.PaxosEntry{
			PropNum: ins.maxProposedNum, Value: propValue}}
	// case 1: reject
	ins.promisedNum = ins.maxProposedNum + 1
	rejectRsp := ins.updateAccepted(msg)
	assert(pb.MsgAccptResp == rejectRsp.Type)
	assert(rejectRsp.Index == msg.Index)
	assert(rejectRsp.From == msg.To)
	assert(rejectRsp.To == msg.From)
	assert(true == rejectRsp.Reject)
	assert(rejectRsp.Entry.PropNum == msg.Entry.PropNum)

	// case 2: accepted
	ins.promisedNum = ins.maxProposedNum
	rsp := ins.updateAccepted(msg)
	assert(pb.MsgAccptResp == rsp.Type)
	assert(rsp.Index == msg.Index)
	assert(false == rsp.Reject)
	assert(rsp.From == msg.To)
	assert(rsp.To == msg.From)
	assert(rsp.Entry.PropNum == msg.Entry.PropNum)
	assert(0 == bytes.Compare(msg.Entry.Value, ins.acceptedValue))
	assert(ins.promisedNum == msg.Entry.PropNum)
	assert(ins.acceptedNum == msg.Entry.PropNum)
}

func TestStepAcceptor(t *testing.T) {
	printIndicate()

	// case 1:
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.proposingValue = RandByte(100)

		remoteIns := randSpaxosInstance()
		remoteIns.index = ins.index
		assert(nil != remoteIns)
		var remoteSp *spaxos
		for {
			remoteSp = randSpaxos()
			assert(nil != remoteSp)
			if sp.id != remoteSp.id {
				break
			}
		}

		remoteIns.chosen = false

		// prepare
		for {
			ins.beginPreparePhase(sp)
			propMsg := sp.outMsgs[0]
			propMsg.To = remoteSp.id
			remoteIns.stepAcceptor(remoteSp, propMsg)

			assert(1 == len(remoteSp.outMsgs))
			propRsp := remoteSp.outMsgs[0]
			remoteSp.outMsgs = nil
			sp.outMsgs = nil
			sp.outHardStates = nil
			if false == propRsp.Reject {
				// promised
				assert(1 == len(remoteSp.outHardStates))
				remoteSp.outHardStates = nil
				break
			}
		}

		// accept
		{
			ins.beginAcceptPhase(sp)
			accptMsg := sp.outMsgs[0]
			accptMsg.To = remoteSp.id
			assert(pb.MsgAccpt == accptMsg.Type)
			remoteIns.stepAcceptor(remoteSp, accptMsg)

			assert(1 == len(remoteSp.outMsgs))
			accptRsp := remoteSp.outMsgs[0]
			remoteSp.outMsgs = nil
			sp.outMsgs = nil
			sp.outHardStates = nil
			assert(false == accptRsp.Reject)
			// accpted
			assert(1 == len(remoteSp.outHardStates))
			hs := remoteSp.outHardStates[0]
			assert(nil != hs.AcceptedValue)
			remoteSp.outHardStates = nil
		}
	}
}

// TEST: proposer
func TestBeginPreparePhase(t *testing.T) {
	printIndicate()

	// case 1: chosen == false
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.proposingValue = RandByte(100)
		assert(nil != ins.proposingValue)
		ins.beginPreparePhase(sp)
		// check ins stat
		assert(ins.maxProposedNum == ins.promisedNum)

		// check msg & hard stat
		assert(1 == len(sp.outMsgs))
		propMsg := sp.outMsgs[0]
		assert(pb.MsgProp == propMsg.Type)
		assert(ins.index == propMsg.Index)
		assert(sp.id == propMsg.From)
		assert(0 == propMsg.To)
		assert(ins.maxProposedNum == propMsg.Entry.PropNum)

		assert(1 == len(sp.outHardStates))
		hs := sp.outHardStates[0]
		{
			newins := rebuildSpaxosInstance(hs)
			assert(nil != newins)
			assert(true == ins.Equal(newins))
		}
	}

	// case 2: chosen == true
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = true
		ins.proposingValue = ins.acceptedValue
		ins.beginPreparePhase(sp)
		assert(0 == len(sp.outMsgs))
		assert(0 == len(sp.outHardStates))
		// TODO: check sp chosen queue
	}
}

func TestBeginAcceptPhase(t *testing.T) {
	printIndicate()

	// case 1: chosen item
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = true
		ins.proposingValue = ins.acceptedValue
		ins.beginAcceptPhase(sp)
		assert(0 == len(sp.outMsgs))
		assert(0 == len(sp.outHardStates))
	}

	// case 2:
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.proposingValue = RandByte(100)
		assert(nil != ins.proposingValue)
		// setting up: proposedNum, promiseNum
		ins.beginPreparePhase(sp)
		ins.beginAcceptPhase(sp)

		// check ins stat
		assert(ins.maxProposedNum == ins.promisedNum)
		assert(ins.maxProposedNum == ins.acceptedNum)
		assert(0 == bytes.Compare(ins.proposingValue, ins.acceptedValue))

		// check msg & hard state
		assert(2 == len(sp.outMsgs))
		accptMsg := sp.outMsgs[1]
		assert(pb.MsgAccpt == accptMsg.Type)
		assert(ins.index == accptMsg.Index)
		assert(sp.id == accptMsg.From)
		assert(0 == accptMsg.To)
		assert(ins.maxProposedNum == accptMsg.Entry.PropNum)
		assert(0 == bytes.Compare(
			ins.proposingValue, accptMsg.Entry.Value))

		assert(2 == len(sp.outHardStates))
		hs := sp.outHardStates[1]
		{
			newins := rebuildSpaxosInstance(hs)
			assert(nil != newins)
			assert(true == ins.Equal(newins))
		}
	}
}

func TestPropose(t *testing.T) {
	printIndicate()

	// case 1:
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		proposingValue := RandByte(100)
		ins.Propose(sp, proposingValue)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		assert(0 == bytes.Compare(proposingValue, ins.proposingValue))
	}

	// case 2:
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = true
		ins.proposingValue = ins.acceptedValue
		proposingValue := RandByte(100)

		ins.Propose(sp, proposingValue)
		assert(0 == len(sp.outMsgs))
		assert(0 == len(sp.outHardStates))
		assert(0 != bytes.Compare(proposingValue, ins.proposingValue))
	}
}

func TestRspVotes(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	cnt := uint64(len(sp.groups))
	// case 1: promisedByMaority
	assert(1 <= cnt)
	var ok bool
	ok = promisedByMajority(sp, randRspVotes(0, cnt/2+1))
	assert(true == ok)
	ok = promisedByMajority(sp, randRspVotes(0, cnt/2))
	assert(false == ok)

	ok = rejectedByMajority(sp, randRspVotes(cnt/2, 0))
	assert(false == ok)
	ok = rejectedByMajority(sp, randRspVotes(cnt/2+1, 0))
	assert(true == ok)
}

func TestMarkChosen(t *testing.T) {
	printIndicate()

	// case 1
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.markChosen(sp, false)
		assert(true == ins.chosen)
		assert(0 == len(sp.outMsgs))
		assert(0 == len(sp.outHardStates))
	}

	// case 2
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.markChosen(sp, true)
		assert(true == ins.chosen)
		assert(1 == len(sp.outMsgs))
		assert(0 == len(sp.outHardStates))

		chosenMsg := sp.outMsgs[0]
		assert(pb.MsgChosen == chosenMsg.Type)
		assert(ins.index == chosenMsg.Index)
		assert(sp.id == chosenMsg.From)
		assert(0 == chosenMsg.To)
		assert(nil != chosenMsg.Entry.Value)
		assert(0 == bytes.Compare(ins.acceptedValue, chosenMsg.Entry.Value))
	}
}

func TestStepPrepareRsp(t *testing.T) {
	// TODO
}

func TestStepAcceptRsp(t *testing.T) {
	// TODO
}