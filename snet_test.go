package spaxos

import (
	"bytes"
	"net"
	"testing"

	pb "spaxos/spaxospb"
)

func TestRunRecvMsg(t *testing.T) {
	svrAddr := ":15001"
	ln, err := net.Listen("tcp", svrAddr)
	if nil != err {
		t.Error(err)
	}

	recvc := make(chan pb.Message)
	go RunRecvMsg(ln, recvc)

	conn, err := net.Dial("tcp", svrAddr)
	if nil != err {
		t.Error(err)
	}

	msg := pb.Message{Type: pb.MsgHup, To: 1, From: 1, Index: 10}
	pkg, err := msg.Marshal()
	if nil != err {
		t.Error(err)
	}

	err = sendMsg(conn, pkg)
	assert(nil == err)

	recvmsg := <-recvc
	{
		newpkg, err := recvmsg.Marshal()
		assert(nil == err)
		ret := bytes.Compare(pkg, newpkg)
		assert(0 == ret)
	}
}
