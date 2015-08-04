package spaxos

import (
	"bytes"
	"net"
	"testing"

	pb "spaxos/spaxospb"
)

func TestRunRecvMsg(t *testing.T) {
	groupsid := []uint64{1}
	peers := []string{":16001"}
	s := newSNet(1, groupsid, peers)

	err := s.RunRecvMsg()
	assert(nil == err)

	conn, err := net.Dial("tcp", peers[0])
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

	recvmsg := <-s.recvc
	{
		newpkg, err := recvmsg.Marshal()
		assert(nil == err)
		ret := bytes.Compare(pkg, newpkg)
		assert(0 == ret)
	}
}
