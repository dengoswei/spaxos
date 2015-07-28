package spaxos

import (
	pb "spaxos/spaxospb"
)

type State struct {
	// stat need to be store into stable storage
	state *HardState

	// msg need to be send out/back
	msg *pb.Message
}
