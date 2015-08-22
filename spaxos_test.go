package spaxos

import (
	"bytes"
	"fmt"
	"testing"

	pb "spaxos/spaxospb"
)

var _ = fmt.Println

func TestNewPaxos(t *testing.T) {
	printIndicate()

	groups := make(map[uint64]bool)
	groups[1] = true
	groups[2] = true
	groups[3] = true

	sp := randSpaxos()
	assert(nil != sp)
	assert(nil != sp.chosenMap)
	assert(nil != sp.insgroup)
	assert(nil != sp.done)
	assert(nil != sp.stop)
	assert(nil != sp.timeoutQueue)
	assert(nil != sp.tickc)
	assert(nil != sp.propc)
	assert(nil != sp.storec)
	assert(nil != sp.sendc)
	assert(nil != sp.recvc)
}

func TestSubmitChosen(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)
	assert(0 == len(sp.chosenMap))

	sp.maxIndex = 10
	sp.minIndex = 0
	sp.submitChosen(1)
	assert(1 == len(sp.chosenMap))
	b, ok := sp.chosenMap[1]
	assert(true == ok)
	assert(true == b)
	assert(1 == sp.nextMinIndex)

	sp.submitChosen(3)
	assert(2 == len(sp.chosenMap))
	assert(1 == sp.nextMinIndex)
	sp.submitChosen(2)
	assert(3 == len(sp.chosenMap))
	assert(3 == sp.nextMinIndex)
}

func TestAppend(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)
	assert(0 == len(sp.outMsgs))
	assert(0 == len(sp.outHardStates))

	msg := pb.Message{
		Type: pb.MsgProp, Index: 10, From: sp.id}
	sp.appendMsg(msg)
	assert(1 == len(sp.outMsgs))
	outMsg := sp.outMsgs[0]
	assert(msg.Type == outMsg.Type)
	assert(msg.Index == outMsg.Index)

	hs := randHardState()
	assert(0 != hs.Index)
	sp.appendHardState(hs)
	assert(1 == len(sp.outHardStates))
	outHs := sp.outHardStates[0]
	assert(true == hs.Equal(&outHs))
}

func TestGetNextProposeNum(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)
	assert(0 != sp.id)

	nextProp := sp.getNextProposeNum(0, 0)
	assert(nextProp == sp.id)

	nextProp = sp.getNextProposeNum(nextProp, 0)
	assert(nextProp == MaxNodeID+sp.id)

	nextProp = sp.getNextProposeNum(0, MaxNodeID+sp.id+1)
	assert(nextProp == MaxNodeID*2+sp.id)
}

func TestSimplePropose(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	defer sp.Stop()
	go sp.runStateMachine()

	reqid, values := randPropValue(1)
	sp.multiPropose(reqid, values, false)

	// wait for
	spkg := <-sp.storec
	assert(1 == len(spkg.outMsgs))
	assert(1 == len(spkg.outHardStates))

	assert(0 == sp.minIndex)
	assert(uint64(1) == sp.maxIndex)
	assert(1 == len(sp.insgroup))
	ins := sp.getSpaxosInstance(sp.maxIndex)
	assert(nil != ins)
	assert(sp.maxIndex == ins.index)

	{
		msg := spkg.outMsgs[0]
		assert(pb.MsgProp == msg.Type)
		assert(ins.index == msg.Index)
		assert(ins.promisedNum == ins.maxProposedNum)
		assert(1 == len(ins.proposingValue.Values))
		{
			preqid := ins.proposingValue.Reqid
			pvalues := ins.proposingValue.Values
			assert(reqid == preqid)
			assert(1 == len(pvalues))
			assert(len(values) == len(pvalues))
			assert(0 == bytes.Compare(values[0], pvalues[0]))
		}

		hs := spkg.outHardStates[0]
		assert(hs.Index == ins.index)
		assert(false == hs.Chosen)
		assert(ins.maxProposedNum == hs.MaxProposedNum)
		assert(ins.promisedNum == hs.MaxPromisedNum)
	}

	// promised
	for {
		if ins.isPromised {
			break
		}

		propRsp := randPropRsp(sp, ins)
		sp.recvc <- propRsp
	}

	// wait for
	spkg = <-sp.storec
	assert(1 == len(spkg.outMsgs))
	assert(1 == len(spkg.outHardStates))
	{
		msg := spkg.outMsgs[0]
		assert(pb.MsgAccpt == msg.Type)
		assert(ins.index == msg.Index)
		assert(ins.acceptedNum == ins.promisedNum)
		assert(ins.acceptedNum == ins.maxProposedNum)

		hs := spkg.outHardStates[0]
		assert(hs.Index == ins.index)
		assert(false == hs.Chosen)
		assert(hs.Index == ins.index)
		assert(true == hs.AcceptedValue.Equal(ins.acceptedValue))
	}

	// accepted
	for {
		if ins.chosen {
			break
		}

		accptRsp := randAccptRsp(sp, ins)
		sp.recvc <- accptRsp
	}

	assert(true == ins.chosen)
	assert(1 == sp.nextMinIndex)
	spkg = <-sp.storec
	assert(1 == spkg.minIndex)
	println(len(spkg.outMsgs), len(spkg.outHardStates))
	LogDebug("%+v", spkg.outMsgs[0])
	LogDebug("%+v", spkg.outMsgs)
	assert(1 == len(spkg.outMsgs))
	assert(0 == len(spkg.outHardStates))
	{
		msg := spkg.outMsgs[0]
		assert(pb.MsgChosen == msg.Type)
	}

	//	cits := <-sp.chosenc
	//	assert(1 == len(cits))
	//	{
	//		hs := cits[0]
	//		assert(true == hs.Chosen)
	//		assert(hs.Index == ins.index)
	//		assert(ins.maxProposedNum == hs.MaxProposedNum)
	//		assert(ins.promisedNum == hs.MaxPromisedNum)
	//		assert(ins.acceptedNum == hs.MaxAcceptedNum)
	//		assert(true == hs.AcceptedValue.Equal(ins.acceptedValue))
	//
	//		acceptedValue := hs.AcceptedValue
	//		assert(1 == len(acceptedValue.Values))
	//		{
	//			areqid := acceptedValue.Reqid
	//			avalues := acceptedValue.Values
	//			assert(reqid == areqid)
	//			assert(1 == len(avalues))
	//			assert(len(values) == len(avalues))
	//			assert(0 == bytes.Compare(values[0], avalues[0]))
	//		}
	//	}
}

//
func TestRunStorage(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	db := NewFakeStorage()
	assert(nil != db)

	defer sp.Stop()
	go sp.fakeRunStateMachine()
	go sp.runStorage(db)

	ins := randSpaxosInstance()
	assert(nil != ins)
	ins.chosen = false
	ins.proposingValue = randPropItem()
	{
		ins.beginPreparePhase(sp, false)
		assert(1 == len(sp.outMsgs))
		assert(1 == len(sp.outHardStates))

		storec, spkg := sp.getStorePackage()
		assert(nil != storec)
		assert(1 == len(spkg.outMsgs))
		assert(1 == len(spkg.outHardStates))
		storec <- spkg
		sp.outMsgs = nil
		sp.outHardStates = nil
	}

	sendingMsgs := <-sp.sendc
	assert(0 < len(sendingMsgs))
	{
		for _, msg := range sendingMsgs {
			assert(pb.MsgProp == msg.Type)
			assert(ins.index == msg.Index)
			assert(sp.id == msg.From)
			assert(ins.maxProposedNum == msg.Entry.PropNum)
		}
	}

	newhs, err := db.Get(ins.index)
	assert(nil == err)
	assert(ins.index == newhs.Index)
	{
		newins := rebuildSpaxosInstance(newhs)
		assert(nil != newins)
		assert(true == ins.Equal(newins))
	}
}

func TestRunNetwork(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	fswitch := NewFakeSwitch(sp.id)
	assert(nil != fswitch)

	defer sp.Stop()
	go sp.fakeRunStateMachine()
	go sp.runSwitch(fswitch)

	ins := randSpaxosInstance()
	assert(nil != ins)
	{
		// send msg
		msg := randPropRsp(sp, ins)
		msg.To = msg.From
		msg.From = sp.id
		if msg.To == sp.id {
			if uint64(1) == sp.id {
				msg.To = 2
			} else {
				msg.To = 1
			}
		}
		assert(msg.To != sp.id)

		sp.sendc <- []pb.Message{msg}

		sendmsg := <-fswitch.GetSendChan()
		assert(sendmsg.Type == msg.Type)
		assert(sendmsg.Index == msg.Index)
		assert(sendmsg.From == msg.From)
		assert(sendmsg.To == msg.To)
		assert(sendmsg.Reject == msg.Reject)
		assert(sendmsg.Entry.PropNum == msg.Entry.PropNum)
	}

	{
		// (TODO):forwarding msg testcase ?
	}

	{
		msg := randPropRsp(sp, ins)
		assert(sp.id == msg.To)
		fswitch.GetRecvChan() <- msg

		recvmsg := <-sp.recvc
		assert(recvmsg.Type == msg.Type)
		assert(recvmsg.Index == msg.Index)
		assert(recvmsg.From == msg.From)
		assert(recvmsg.To == msg.To)
		assert(recvmsg.Reject == msg.Reject)
		assert(recvmsg.Entry.PropNum == msg.Entry.PropNum)
	}
}
