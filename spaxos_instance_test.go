package spaxos

import (
	"math/rand"
	"testing"

	//	pb "spaxos/spaxospb"
	pb "github.com/dengoswei/spaxos/spaxospb"
)

func TestNewSpaxosInstance(t *testing.T) {
	println(TestNewSpaxosInstance)

	index := uint64(rand.Uint32())
	ins := newSpaxosInstance(index)
	assert(nil != ins)
	assert(index == ins.index)
	assert(false == ins.chosen)
	assert(0 == ins.maxProposedNum)
	assert(0 == ins.maxAcceptedHintNum)
	assert(nil == ins.proposingValue)
	assert(nil == ins.rspVotes)
	assert(false == ins.tryNoopProp)
	assert(nil == ins.noopVotes)
	assert(nil != ins.stepProposer)
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
	assert(true == hs.AcceptedValue.Equal(ins.acceptedValue))
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
	assert(true == ins.acceptedValue.Equal(newins.acceptedValue))
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
	assert(true == rsp.Entry.Value.Equal(ins.acceptedValue))
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

	propValue := randPropItem()
	assert(nil != propValue)

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
	assert(true == msg.Entry.Value.Equal(ins.acceptedValue))
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
		ins.proposingValue = randPropItem()
		assert(nil != ins.proposingValue)

		remoteIns := randSpaxosInstance()
		remoteIns.index = ins.index
		assert(nil != remoteIns)
		var remoteSp *spaxos
		remoteSp = randSpaxos()
		assert(nil != remoteSp)
		if sp.id == remoteSp.id {
			if uint64(1) == sp.id {
				remoteSp.id = 2
			} else {
				remoteSp.id = 1
			}
		}

		remoteIns.chosen = false

		// prepare
		for {
			remoteIns.promisedNum = ins.maxProposedNum
			remoteIns.acceptedNum = 0
			remoteIns.acceptedValue = nil

			ins.beginPreparePhase(sp, false)
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

		return

		// accept
		{
			ins.beginAcceptPhase(sp, false)
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
		ins.proposingValue = randPropItem()
		assert(nil != ins.proposingValue)
		ins.beginPreparePhase(sp, false)
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
		ins.beginPreparePhase(sp, false)
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
		ins.beginAcceptPhase(sp, false)
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
		ins.proposingValue = randPropItem()
		assert(nil != ins.proposingValue)
		// setting up: proposedNum, promiseNum
		ins.beginPreparePhase(sp, false)
		ins.beginAcceptPhase(sp, false)

		// check ins stat
		assert(ins.maxProposedNum == ins.promisedNum)
		assert(ins.maxProposedNum == ins.acceptedNum)
		assert(true == ins.proposingValue.Equal(ins.acceptedValue))

		// check msg & hard state
		assert(2 == len(sp.outMsgs))
		accptMsg := sp.outMsgs[1]
		assert(pb.MsgAccpt == accptMsg.Type)
		assert(ins.index == accptMsg.Index)
		assert(sp.id == accptMsg.From)
		assert(0 == accptMsg.To)
		assert(ins.maxProposedNum == accptMsg.Entry.PropNum)
		assert(true == ins.proposingValue.Equal(accptMsg.Entry.Value))

		assert(2 == len(sp.outHardStates))
		hs := sp.outHardStates[1]
		{
			newins := rebuildSpaxosInstance(hs)
			assert(nil != newins)
			assert(true == ins.Equal(newins))
		}
	}
}

func TestBeginMasterPreparePhase(t *testing.T) {
	printIndicate()

	// case 1: master prop => skip into accept phase
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.hostPropReqid = 0
		ins.maxProposedNum = 0
		ins.maxAcceptedHintNum = 0
		ins.proposingValue = nil
		ins.promisedNum = 0
		ins.acceptedNum = 0
		ins.acceptedValue = nil

		ins.proposingValue = randPropItem()
		ins.hostPropReqid = ins.proposingValue.Reqid
		ins.tryNoopProp = false
		ins.beginMasterPreparePhase(sp)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))

		accptMsg := sp.outMsgs[0]
		assert(pb.MsgAccpt == accptMsg.Type)
		assert(ins.index == accptMsg.Index)
		assert(sp.id == accptMsg.From)
		assert(0 == accptMsg.To)
		assert(0 == accptMsg.Entry.PropNum)
		assert(nil != accptMsg.Entry.Value)
		assert(0 == ins.promisedNum)
		assert(0 == ins.acceptedNum)
	}

	// case 2: master prop => backoff into normal propose
	{
		ins := randSpaxosInstance()
		assert(nil != ins)
		sp := randSpaxos()
		assert(nil != sp)

		ins.chosen = false
		ins.hostPropReqid = 0
		ins.maxProposedNum = 0
		ins.proposingValue = randPropItem()
		ins.hostPropReqid = ins.proposingValue.Reqid
		ins.tryNoopProp = false
		ins.beginMasterPreparePhase(sp)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		propMsg := sp.outMsgs[0]
		assert(pb.MsgProp == propMsg.Type)
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
		proposingValue := randPropItem()
		ins.acceptedNum = 0
		ins.acceptedValue = nil
		assert(nil != proposingValue)
		ins.Propose(sp, proposingValue, false)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		assert(true == proposingValue.Equal(ins.proposingValue))
	}

	// case 2:
	// => never proposing on a local chosen spaxos instance:
	//    in face: never re-do proposing on one single spaxos instance unless no-op propose;
	//	{
	//		sp := randSpaxos()
	//		assert(nil != sp)
	//
	//		ins := randSpaxosInstance()
	//		assert(nil != ins)
	//
	//		ins.chosen = true
	//		ins.proposingValue = ins.acceptedValue
	//		proposingValue := randPropItem()
	//		assert(nil != proposingValue)
	//
	//		ins.Propose(sp, proposingValue, false)
	//		assert(0 == len(sp.outMsgs))
	//		assert(0 == len(sp.outHardStates))
	//		assert(false == proposingValue.Equal(ins.proposingValue))
	//	}
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
		assert(true == ins.acceptedValue.Equal(chosenMsg.Entry.Value))
	}
}

func helpMajorPromised(sp *spaxos, ins *spaxosInstance) {
	ins.chosen = false
	ins.proposingValue = ins.acceptedValue
	ins.beginPreparePhase(sp, false)
	assert(1 == len(sp.outMsgs))
	assert(1 == len(sp.outHardStates))
	sp.outMsgs = nil
	sp.outHardStates = nil

	// phase 1: promised
	for {
		if 1 == len(sp.outMsgs) {
			msgReq := sp.outMsgs[0]
			assert(pb.MsgAccpt == msgReq.Type)
			break
		}

		propRsp := randPropRsp(sp, ins)
		ins.stepProposer(sp, propRsp)
	}
}

func helpMajorRejected(sp *spaxos, ins *spaxosInstance) {
	ins.chosen = false
	ins.proposingValue = ins.acceptedValue
	ins.beginPreparePhase(sp, false)
	sp.outMsgs = nil
	sp.outHardStates = nil

	prevPropNum := ins.maxProposedNum
	// phase 1: reject
	rejectCnt := 0
	for {
		if rejectCnt > len(ins.rspVotes) {
			break
		}

		rejectCnt = len(ins.rspVotes)

		propRsp := randPropRsp(sp, ins)
		propRsp.Reject = true
		ins.stepProposer(sp, propRsp)
	}

	assert(prevPropNum < ins.maxProposedNum)
}

func TestStepPrepareRsp(t *testing.T) {
	// case 1
	{
		ins := randSpaxosInstance()
		assert(nil != ins)

		sp := randSpaxos()
		assert(nil != sp)

		helpMajorPromised(sp, ins)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		accptMsg := sp.outMsgs[0]
		assert(pb.MsgAccpt == accptMsg.Type)
		assert(ins.index == accptMsg.Index)
		assert(sp.id == accptMsg.From)
		assert(0 == accptMsg.To)
		assert(ins.maxProposedNum == accptMsg.Entry.PropNum)
		assert(ins.proposingValue.Equal(accptMsg.Entry.Value))

		hs := sp.outHardStates[0]
		{
			newins := rebuildSpaxosInstance(hs)
			assert(true == ins.Equal(newins))
		}
	}

	// case 2
	{
		ins := randSpaxosInstance()
		assert(nil != ins)

		sp := randSpaxos()
		assert(nil != sp)

		helpMajorRejected(sp, ins)

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		propMsg := sp.outMsgs[0]
		assert(pb.MsgProp == propMsg.Type)
		assert(ins.index == propMsg.Index)
		assert(sp.id == propMsg.From)
		assert(0 == propMsg.To)
		assert(ins.maxProposedNum == propMsg.Entry.PropNum)

		hs := sp.outHardStates[0]
		{
			newins := rebuildSpaxosInstance(hs)
			assert(true == ins.Equal(newins))
		}
	}

	// case 3
	{
	}
	// TODO
}

func TestStepAcceptRsp(t *testing.T) {
	// case 1: succ chosen
	{
		ins := randSpaxosInstance()
		assert(nil != ins)

		sp := randSpaxos()
		assert(nil != sp)

		helpMajorPromised(sp, ins)
		sp.outMsgs = nil
		sp.outHardStates = nil
		for {
			if ins.chosen {
				break
			}

			accptRsp := randAccptRsp(sp, ins)
			ins.stepProposer(sp, accptRsp)
		}

		assert(true == ins.chosen)
		assert(1 == len(sp.outMsgs))

		chosenMsg := sp.outMsgs[0]
		assert(pb.MsgChosen == chosenMsg.Type)
		assert(ins.index == chosenMsg.Index)
		assert(sp.id == chosenMsg.From)
		assert(0 == chosenMsg.To)
		assert(nil != chosenMsg.Entry.Value)
		assert(ins.acceptedValue.Equal(chosenMsg.Entry.Value))
	}

	// case 2: fail to chosen
	{
		ins := randSpaxosInstance()
		assert(nil != ins)

		sp := randSpaxos()
		assert(nil != sp)

		helpMajorPromised(sp, ins)

		prevPropNum := ins.maxProposedNum
		sp.outMsgs = nil
		sp.outHardStates = nil
		for {
			if 1 == len(sp.outMsgs) {
				msgReq := sp.outMsgs[0]
				assert(pb.MsgProp == msgReq.Type)
				break
			}

			accptRsp := randAccptRsp(sp, ins)
			accptRsp.Reject = true
			ins.stepProposer(sp, accptRsp)
		}

		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))
		assert(prevPropNum < ins.maxProposedNum)
		propMsg := sp.outMsgs[0]
		assert(pb.MsgProp == propMsg.Type)
		assert(ins.index == propMsg.Index)
		assert(sp.id == propMsg.From)
		assert(0 == propMsg.To)
		assert(ins.maxProposedNum == propMsg.Entry.PropNum)
	}
}

func TestNoopPropose(t *testing.T) {
	printIndicate()

	ins := randSpaxosInstance()
	assert(nil != ins)
	sp := randSpaxos()
	assert(nil != sp)

	// self accepted
	ins.chosen = false
	proposingValue := randPropItem()
	assert(nil != proposingValue)
	ins.acceptedNum = 0
	ins.acceptedValue = nil
	ins.Propose(sp, proposingValue, false)
	assert(false == ins.tryNoopProp)

	// backoff to no-op prop
	ins.NoopPropose(sp)
	assert(nil == ins.proposingValue)
	assert(true == ins.tryNoopProp)
	assert(nil != ins.rspVotes)
	assert(0 == len(ins.rspVotes))
	assert(nil != ins.noopVotes)
	assert(0 == len(ins.rspVotes))

	sp.outMsgs = nil
	sp.outHardStates = nil
	for {
		if 1 == len(sp.outMsgs) {
			msgReq := sp.outMsgs[0]
			assert(pb.MsgAccpt == msgReq.Type)
			assert(nil == msgReq.Entry.Value)
			break
		}

		noopPropRsp := randPropRsp(sp, ins)
		ins.stepProposer(sp, noopPropRsp)
	}

	sp.outMsgs = nil
	sp.outHardStates = nil
	for {
		if ins.chosen {
			break
		}

		accptRsp := randAccptRsp(sp, ins)
		ins.stepProposer(sp, accptRsp)
	}

	assert(true == ins.chosen)
	assert(1 == len(sp.outMsgs))

	assert(nil == ins.acceptedValue)
	chosenMsg := sp.outMsgs[0]
	assert(pb.MsgChosen == chosenMsg.Type)
	assert(ins.index == chosenMsg.Index)
	assert(sp.id == chosenMsg.From)
	assert(0 == chosenMsg.To)
	assert(nil == chosenMsg.Entry.Value)
}
