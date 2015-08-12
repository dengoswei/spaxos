package spaxos

import (
	pb "spaxos/spaxospb"
)

const MaxNodeID uint64 = 1024

type storePackage struct {
	outMsgs       []pb.Message
	outHardStates []pb.HardState
}

type Storage interface {
	// store hard state
	Store([]pb.HardState) error

	Get(index uint64) (pb.HardState, error)
}

// TODO: fix interface func & name!!!
type Network interface {
	Send([]pb.Message) (int, error)
	Recv() ([]pb.Message, error)
}
