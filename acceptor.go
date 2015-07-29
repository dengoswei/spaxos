package spaxos

import (
	"errnors"

	pb "spaxos/spaxospb"
)

type stepFunc func(ins *spaxosInstance, msg *pb.Message) (bool, error)

type roleAcceptor struct {
	//
	step stepFunc

	//
	promisedCnt    uint32
	acceptedCnt    uint32
	maxPromisedNum uint64
	maxAcceptedNum uint64
	acceptedValue  []byte
}

func (a *roleAcceptor) stepByMsgProp(ins *spaxosInstance, msg *pb.Message) (bool, error) {
	assert(msg.Index == ins.index)

	rsp := &pb.Message{
		Type: pb.MsgPropResp, Reject: false,
		Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}
	if msg.Entry.PropNum < a.maxPromisedNum {
		rsp.Reject = true
		ins.msgs.append(rsp)
		return true, nil
	}

	if 0 != a.maxAcceptedNum {
		rsp.Entry.AccptNum = a.maxAcceptedNum
		rsp.Entry.Value = a.acceptedValue
	}

	a.promisedCnt += 1
	if msg.Entry.PropNum == a.maxPromisedNum {
		ins.msgs.append(rsp)
		return true, nil
	}

	a.maxPromisedNum = msg.Entry.PropNum
	hs := &pb.HardState{
		Type:           pb.HardStateAccpt,
		Index:          ins.index,
		MaxPromisedNum: a.maxPromisedNum,
		MaxAcceptedNum: a.maxAcceptedNum,
		AcceptedValue:  a.acceptedValue}

	ins.hss.append(hs)
	ins.msgs.append(rsp)
	return true, nil
}

func (a *roleAcceptor) stepByMsgAccpt(ins *spaxosInstance, msg *pb.Message) (bool, error) {
	assert(msg.Index == ins.index)

	rsp := &pb.Message{
		Type: pb.MsgAccptResp, Reject: false,
		Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}

	if msg.Entry.PropNum < a.maxPromisedNum {
		rsp.Reject = true
		ins.msgs.append(rsp)
		return true, nil
	}

	a.acceptedCnt += 1
	if msg.Entry.PropNum == a.maxAcceptedNum {
		ins.msgs.append(rsp)
		return true, nil // do not repeat produce the same hs
	}

	assert(msg.Entry.PropNum > a.maxAcceptedNum)
	a.maxPromisedNum = msg.Entry.PropNum
	a.maxAcceptedNum = msg.Entry.PropNum
	a.acceptedValue = msg.Entry.Value

	hs := &pb.HardState{
		Type:           pb.HardStateAccpt,
		Index:          ins.index,
		MaxPromisedNum: a.maxPromisedNum,
		MaxAcceptedNum: a.maxAcceptedNum,
		AcceptedValue:  a.acceptedValue}
	ins.hss.append(hs)
	ins.msgs.append(rsp)
	return true, nil
}

func (a *roleAcceptor) step(ins *spaxosInstance, msg *pb.Message) (bool, error) {
	if msg.Index != ins.index {
		return true, nil // simple ignore
	}

	switch msg.Type {
	case pb.MsgProp:
		return a.stepByMsgProp(ins, msg)
	case pb.MsgAccpt:
		return a.stepByMsgAccpt(ins, msg)
	default:
		return false, error.New("acceptor: error msg type")
	}
}
