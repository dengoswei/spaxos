package spaxos

import (
	"errnors"

	pb "spaxos/spaxospb"
)

type AcceptorStatus int
type stepAcceptFunc func(msg *pb.Message) (State, error)

const (
	AcceptorNil          AcceptorStatus = 0
	AcceptorPromiseNil   AcceptorStatus = 1
	AcceptorPromiseValue AcceptorStatus = 2
)

type roleAcceptor struct {
	//
	status AcceptorStatus
	step   stepAcceptFunc

	//
	promisedCnt    uint32
	acceptedCnt    uint32
	maxPromisedNum uint64
	maxAcceptedNum uint64
	acceptedValue  []byte
}

func newAcceptor() *roleAcceptor {
	return &roleAcceptor{
		status: AcceptorNil,
		step:   stepNil,
	}
}

func (acceptor *roleAcceptor) stepNil(msg *pb.Message) (State, error) {
	assert(AcceptorNil == acceptor.status)
	assert(0 == acceptor.maxPromisedNum)
	assert(0 == acceptor.maxAcceptedNum)

	if pb.MsgProp != msg.Type {
		return State{}, error.New("spaxos: unexpected msgtype")
	}

	rspMsg := pb.Message{
		Type:   pb.MsgPropResp,
		Reject: false,
		Entry:  pb.PaxosEntry{PropNum: msg.Entry.PropNum}}
	if 0 == msg.Entry.PropNum {
		rspMsg.Reject = true
		return State{msg: &rspMsg}, nil
	}

	acceptor.promisedCnt = 1
	acceptor.maxPromisedNum = msg.Entry.PropNum
	acceptor.status = AcceptorPromiseNil
	acceptor.step = stepPromiseNil
	return State{
		state: &HardState{
			maxPromisedNum: acceptor.maxPromisedNum}, msg: &rspMsg}, nil
}

func (acceptor *roleAcceptor) stepPromiseNil(msg *pb.Message) (State, error) {
	assert(AcceptorPromiseNil == acceptor.status)
	assert(nil == acceptor.acceptedValue)

	state := &pb.HardState{}
	rspMsg := pb.Message{
		Reject: false, Entry: pb.PaxosEntry{PropNum: msg.Entry.PropNum}}
	switch msg.Type {
	case pb.MsgProp:
		rspMsg.Type = pb.MsgPropResp
		if msg.Entry.PropNum <= acceptor.maxPromisedNum {
			rspMsg.Reject = true
			state = nil
			break
		}

		acceptor.promisedCnt += 1
		acceptor.maxPromisedNum = msg.Entry.PropNum
		state.maxPromisedNum = acceptor.maxPromisedNum
	case pb.MsgAccpt:
		rspMsg.Type = pb.MsgAccptResp
		if msg.Entry.PropNum <= acceptor.maxPromisedNum {
			rspMsg.Reject = true
			state = nil
			break
		}

		assert(0 == acceptor.acceptedCnt)
		assert(0 == acceptor.maxAcceptedNum)
		acceptor.acceptedCnt = 1
		acceptor.maxPromisedNum = msg.Entry.PropNum
		acceptor.maxAcceptedNum = acceptor.maxPromisedNum
		acceptor.acceptedValue = msg.Entry.Value

		acceptor.status = AcceptorPromiseValue
		acceptor.step = stepPromiseValue

		state.maxPromisedNum = acceptor.maxPromisedNum
		state.maxAcceptedNum = acceptor.maxAcceptedNum
		state.acceptedValue = acceptor.acceptedValue
	default:
		return State{}, error.New("spaxos: unexpected msg type")
	}

	return State{state: state, msg: &rspMsg}, nil
}

func (acceptor *roleAcceptor) stepPromiseValue(msg *pb.Message) (State, error) {
	assert(AcceptorPromiseValue == acceptor.status)
	assert(0 != acceptor.maxPromisedNum)
	assert(0 != acceptor.maxAcceptedNum)

	if pb.MsgAccpt != msg.Type {
		return State{}, error.New("spaos: unexpected msg type")
	}

	rspMsg := pb.Message{
		Type:   pb.MsgAccptResp,
		Reject: false,
		Entry:  pb.PaxosEntry{PropNum: msg.Entry.PropNum}}

	if msg.Entry.PropNum <= acceptor.maxPromisedNum {
		rspMsg.Reject = true
		return State{msg: &rspMsg}, nil
	}

	assert(acceptor.maxPromisedNum >= acceptor.maxAcceptedNum)
	acceptor.acceptedCnt += 1
	acceptor.maxPromisedNum = msg.Entry.PropNum
	acceptor.maxAcceptedNum = acceptor.maxPromisedNum
	acceptor.acceptedValue = msg.Entry.Value

	// still in AcceptorPromiseValue state
	return State{
		state: &pb.HardState{
			maxPromisedNum: acceptor.maxPromisedNum,
			maxAcceptedNum: acceptor.maxAcceptedNum,
			acceptedValue:  acceptor.acceptedValue}}, nil
}

func (acceptor *roleAcceptor) feed(msg *pb.Message) (State, error) {
	if nil == acceptor.step {
		return State{}, error.New("spaxos: feed acceptor no step")
	}

	return acceptor.step(msg)
}
