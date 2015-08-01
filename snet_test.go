package spaxos

import (
	"bufio"
	"fmt"
	"net"
	"testing"

	pb "spaxos/spaxospb"
)

func TestRunRecvMsg(t *testing.T) {
	svrAddr := ":5001"
	ln, err := net.Listen("tcp", svrAddr)
	if nil != err {
		t.Error(err)
	}

	var recvc chan pb.Message
	go RunRecvMsg(ln, recvc)

	conn, err := net.Dial("tcp", svrAddr)
	if nil != err {
		t.Error(err)
	}

	writer := bufio.NewWriter(conn)
	msg := pb.Message{Type: pb.MsgHup, To: 1, From: 1, Index: 10}
	pkg, err := msg.Marshal()
	if nil != err {
		t.Error(err)
	}

	err = sendMsg(writer, pkg)

	recvmsg := <-recvc
	if recvmsg.Type != msg.Type ||
		recvmsg.To != msg.To || recvmsg.From != msg.From ||
		recvmsg.Index != msg.Index {
		t.Errorf("recvmsg != msg")
	}

	fmt.Printf("success")
}
