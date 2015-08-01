package spaxos

import (
	"errors"

	pb "spaxos/spaxospb"
)

type roleAcceptor struct {
	//
	promisedCnt    uint32
	acceptedCnt    uint32
	maxPromisedNum uint64
	maxAcceptedNum uint64
	acceptedValue  []byte
}

func rebuildAcceptor(hs pb.HardState) *roleAcceptor {
	a := &roleAcceptor{
		maxPromisedNum: hs.MaxPromisedNum,
		maxAcceptedNum: hs.MaxAcceptedNum,
		acceptedValue:  hs.AcceptedValue}
	return a
}

func (a *roleAcceptor) stepByMsgProp(ins *spaxosInstance, msg pb.Message) (bool, error) {
	assert(msg.Index == ins.index)

	rsp := pb.Message{
		Type: pb.MsgPropResp, Reject: false,
		Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}
	if msg.Entry.PropNum < a.maxPromisedNum {
		rsp.Reject = true
		ins.append(rsp)
		return true, nil
	}

	if 0 != a.maxAcceptedNum {
		rsp.Entry.AccptNum = a.maxAcceptedNum
		rsp.Entry.Value = a.acceptedValue
	}

	a.promisedCnt += 1
	if msg.Entry.PropNum == a.maxPromisedNum {
		ins.append(rsp)
		return true, nil
	}

	a.maxPromisedNum = msg.Entry.PropNum

	ins.updateAccptHardState(a.maxPromisedNum, a.maxAcceptedNum, a.acceptedValue)
	ins.append(rsp)
	return true, nil
}

func (a *roleAcceptor) stepByMsgAccpt(ins *spaxosInstance, msg pb.Message) (bool, error) {
	assert(msg.Index == ins.index)

	rsp := pb.Message{
		Type: pb.MsgAccptResp, Reject: false,
		Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}

	if msg.Entry.PropNum < a.maxPromisedNum {
		rsp.Reject = true
		ins.append(rsp)
		return true, nil
	}

	a.acceptedCnt += 1
	if msg.Entry.PropNum == a.maxAcceptedNum {
		ins.append(rsp)
		return true, nil // do not repeat produce the same hs
	}

	assert(msg.Entry.PropNum > a.maxAcceptedNum)
	a.maxPromisedNum = msg.Entry.PropNum
	a.maxAcceptedNum = msg.Entry.PropNum
	a.acceptedValue = msg.Entry.Value

	ins.updateAccptHardState(a.maxPromisedNum, a.maxAcceptedNum, a.acceptedValue)
	ins.append(rsp)
	return true, nil
}

func (a *roleAcceptor) step(ins *spaxosInstance, msg pb.Message) (bool, error) {
	if msg.Index != ins.index {
		return true, nil // simple ignore
	}

	switch msg.Type {
	case pb.MsgProp:
		return a.stepByMsgProp(ins, msg)
	case pb.MsgAccpt:
		return a.stepByMsgAccpt(ins, msg)
	}

	return false, errors.New("acceptor: error msg type")
}
