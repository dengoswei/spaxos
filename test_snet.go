package spaxos

import (
	"net"
	pb "spaxos/spaxospb"
	"testing"
)

func TestRunRecvMsg(t *testing.T) {
	svrAddr := ":5001"
	ln, err := net.Listen("tcp", svrAddr)
	assert(nil == err)

	var recvc chan pb.Message
	go RunRecvMsg(ln, recvc)

	conn, err := net.Dial("tcp", svrAddr)
	assert(nil == err)

	msg := pb.Message{Type: pb.MsgHup, To: 1, From: 1, Index: 10}

}
