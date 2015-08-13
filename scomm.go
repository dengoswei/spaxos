package spaxos

import (
	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type storePackage struct {
	outMsgs       []pb.Message
	outHardStates []pb.HardState
}

type Storager interface {
	// store hard state
	Store([]pb.HardState) error

	Get(index uint64) (pb.HardState, error)
}

// TODO: fix interface func & name!!!
type Networker interface {
	GetSendChan() chan pb.Message
	GetRecvChan() chan pb.Message
}
