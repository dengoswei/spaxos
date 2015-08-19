package spaxos

import (
	"bytes"
	"testing"

	pb "spaxos/spaxospb"
)

func TestNewPaxos(t *testing.T) {
	printIndicate()

	groups := make(map[uint64]bool)
	groups[1] = true
	groups[2] = true
	groups[3] = true

	sp := NewSpaxos(1, groups)
	assert(nil != sp)
	assert(nil != sp.chosenItems)
	assert(nil != sp.insgroup)
	assert(nil != sp.rebuildList)
	assert(nil != sp.propc)
	assert(nil != sp.chosenc)
	assert(nil != sp.storec)
	assert(nil != sp.sendc)
	assert(nil != sp.recvc)
}

func TestSubmitChosen(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)
	assert(0 == len(sp.chosenItems))

	hs := randHardState()
	assert(0 != hs.Index)

	hs.Chosen = true
	sp.submitChosen(hs)
	assert(1 == len(sp.chosenItems))
	chs := sp.chosenItems[hs.Index]
	assert(true == hs.Equal(&chs))

	sp.submitChosen(hs)
	assert(1 == len(sp.chosenItems))
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

func TestGenerateRebuildMsg(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	index := uint64(10)
	msg := sp.generateRebuildMsg(index)
	assert(pb.MsgInsRebuild == msg.Type)
	assert(index == msg.Index)
	assert(msg.To == sp.id)
	assert(msg.From == sp.id)
}

func TestHandOnMsg(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)
	assert(0 == len(sp.outMsgs))
	assert(0 == len(sp.rebuildList))

	msg := pb.Message{
		Type: pb.MsgProp, Index: 10, To: sp.id}
	sp.handOnMsg(msg)
	assert(1 == len(sp.outMsgs))
	assert(1 == len(sp.rebuildList))
	assert(1 == len(sp.rebuildList[msg.Index]))

	outMsg := sp.outMsgs[0]
	assert(pb.MsgInsRebuild == outMsg.Type)

	msg.Type = pb.MsgAccpt
	sp.handOnMsg(msg)
	assert(1 == len(sp.outMsgs))
	assert(1 == len(sp.rebuildList))
	assert(2 == len(sp.rebuildList[msg.Index]))
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

	go sp.runStateMachine()

	reqMap := randPropValue(1)
	sp.multiPropose(reqMap, false)

	// wait for
	spkg := <-sp.storec
	assert(1 == len(spkg.outMsgs))
	assert(1 == len(spkg.outHardStates))

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
			v := ins.proposingValue.Values[0]
			_, ok := reqMap[v.Reqid]
			assert(true == ok)
			assert(0 == bytes.Compare(v.Value, reqMap[v.Reqid]))
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
	spkg = <-sp.storec
	assert(1 == len(spkg.outMsgs))
	assert(0 == len(spkg.outHardStates))
	{
		msg := spkg.outMsgs[0]
		assert(pb.MsgChosen == msg.Type)
	}

	cits := <-sp.chosenc
	assert(1 == len(cits))
	{
		hs := cits[0]
		assert(true == hs.Chosen)
		assert(hs.Index == ins.index)
		assert(ins.maxProposedNum == hs.MaxProposedNum)
		assert(ins.promisedNum == hs.MaxPromisedNum)
		assert(ins.acceptedNum == hs.MaxAcceptedNum)
		assert(true == hs.AcceptedValue.Equal(ins.acceptedValue))

		acceptedValue := hs.AcceptedValue
		assert(1 == len(acceptedValue.Values))
		{
			v := acceptedValue.Values[0]
			_, ok := reqMap[v.Reqid]
			assert(true == ok)
			assert(0 == bytes.Compare(v.Value, reqMap[v.Reqid]))
		}
	}
}

//
func TestRunStorage(t *testing.T) {
	printIndicate()

	sp := randSpaxos()
	assert(nil != sp)

	db := NewFakeStorage()
	assert(nil != db)

	go sp.runStorage(db)

	ins := randSpaxosInstance()
	assert(nil != ins)
	ins.chosen = false
	ins.proposingValue = randPropItem()
	{
		ins.beginPreparePhase(sp)
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

	fnet := NewFakeNetwork(sp.id)
	assert(nil != fnet)

	go sp.runNetwork(fnet)

	ins := randSpaxosInstance()
	assert(nil != ins)
	{
		// send msg
		msg := randPropRsp(sp, ins)
		msg.To = msg.From
		msg.From = sp.id
		sp.sendc <- []pb.Message{msg}

		sendmsg := <-fnet.GetSendChan()
		assert(sendmsg.Type == msg.Type)
		assert(sendmsg.Index == msg.Index)
		assert(sendmsg.From == msg.From)
		assert(sendmsg.To == msg.To)
		assert(sendmsg.Reject == msg.Reject)
		assert(sendmsg.Entry.PropNum == msg.Entry.PropNum)
	}

	{
		// forwarding msg
		hs := randHardState()
		assert(0 < hs.Index)
		msg := pb.Message{
			Type: pb.MsgInsRebuildResp, Hs: hs,
			To: sp.id, From: sp.id, Index: hs.Index}
		sp.sendc <- []pb.Message{msg}

		forwardmsg := <-sp.recvc
		assert(forwardmsg.Type == msg.Type)
		assert(forwardmsg.Index == msg.Index)
		assert(forwardmsg.From == msg.From)
		assert(forwardmsg.To == msg.To)
		assert(true == forwardmsg.Hs.Equal(&(msg.Hs)))
	}

	{
		msg := randPropRsp(sp, ins)
		assert(sp.id == msg.To)
		fnet.GetRecvChan() <- msg

		recvmsg := <-sp.recvc
		assert(recvmsg.Type == msg.Type)
		assert(recvmsg.Index == msg.Index)
		assert(recvmsg.From == msg.From)
		assert(recvmsg.To == msg.To)
		assert(recvmsg.Reject == msg.Reject)
		assert(recvmsg.Entry.PropNum == msg.Entry.PropNum)
	}
}
