package spaxos

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	pb "spaxos/spaxospb"
)

func assert(ok bool) {
	if !ok {
		panic("assert failed")
	}
}

type SNet struct {
	selfid uint64
	recvc  chan pb.Message
	sendc  chan []pb.Message

	// all peers
	groupsid []uint64
	peers    []string
}

func newSNet(selfid uint64, groupsid []uint64, peers []string) *SNet {
	s := &SNet{
		selfid:   selfid,
		groupsid: groupsid,
		peers:    peers,
		recvc:    make(chan pb.Message, 10),
		sendc:    make(chan []pb.Message)}
	return s
}

// simple network wrapper
func handleConnection(
	conn net.Conn, recvc chan pb.Message, groupsid map[uint64]bool) {
	reader := bufio.NewReader(conn)
	for {
		var pkglen int
		{
			var totalLen uint32
			err := binary.Read(reader, binary.BigEndian, &totalLen)
			if nil != err {
				fmt.Println("binary.Read %s", err)
				return
			}
			pkglen = int(totalLen)
			assert(0 <= pkglen)
		}

		var pkg []byte
		for len(pkg) < pkglen {
			value := make([]byte, pkglen-len(pkg))
			readlen, err := reader.Read(value)
			if nil != err {
				return
			}

			assert(0 < readlen)
			pkg = append(pkg, value...)
		}

		assert(pkglen == len(pkg))
		msg := pb.Message{}
		err := msg.Unmarshal(pkg)
		if nil != err {
			return
		}

		if _, ok := groupsid[msg.From]; !ok {
			// invalid msg.From
			continue
		}

		// feed msg into node
		select {
		case recvc <- msg:
		}
	}
}

func (s *SNet) RunRecvMsgImpl(ln net.Listener) {

	groupsid := make(map[uint64]bool)
	for _, id := range s.groupsid {
		groupsid[id] = true
	}

	for {
		conn, err := ln.Accept()
		if nil != err {
			continue
		}

		go handleConnection(conn, s.recvc, groupsid)
	}
}

func (s *SNet) RunRecvMsg() error {
	var lsnAddr string
	assert(len(s.groupsid) == len(s.peers))
	for i := 0; i < len(s.peers); i += 1 {
		if s.selfid == s.groupsid[i] {
			lsnAddr = s.peers[i]
		}
	}

	assert("" != lsnAddr)
	ln, err := net.Listen("tcp", lsnAddr)
	if nil != err {
		return err
	}
	assert(nil != ln)

	go s.RunRecvMsgImpl(ln)
	return nil
}

type peerInfo struct {
	id    uint64
	addr  string
	recvc chan pb.Message
}

func sendMsg(conn net.Conn, pkg []byte) error {

	buf := new(bytes.Buffer)
	buf.Grow(4)
	{
		writelen := uint32(len(pkg))
		err := binary.Write(buf, binary.BigEndian, writelen)
		if nil != err {
			fmt.Println("binary.Write %s", err)
			return err
		}
	}

	writelen, err := conn.Write(append(buf.Bytes(), pkg...))
	if nil != err {
		fmt.Println("conn.Write %s", err)
		return err
	}

	assert(writelen == 4+len(pkg))
	return nil
}

func doSendMsg(conn net.Conn, p peerInfo) error {
	for {
		msg := <-p.recvc
		assert(p.id == msg.To)

		pkg, err := msg.Marshal()
		if nil != err {
			continue
		}

		err = sendMsg(conn, pkg)
		if nil != err {
			return err
		}
	}

	// should never go there
	return nil
}

func handleSendMsg(p peerInfo) {
	for {
		conn, err := net.Dial("tcp", p.addr)
		if nil != err {
			//
			time.Sleep(5 * time.Second)
			continue
		}

		println("handleSendMsg Dial succ")
		doSendMsg(conn, p)
	}
}

func (s *SNet) RunSendMsg() {
	pgroup := make(map[uint64]peerInfo)
	assert(len(s.groupsid) == len(s.peers))

	for i := 0; i < len(s.groupsid); i += 1 {
		id := s.groupsid[i]
		p := peerInfo{
			id: id, addr: s.peers[i], recvc: make(chan pb.Message)}
		pgroup[id] = p
		go handleSendMsg(p)
	}

	msgQueue := make(map[uint64][]pb.Message)
	for {
		select {
		case msgs := <-s.sendc:
			for _, msg := range msgs {
				msgQueue[msg.To] = append(msgQueue[msg.To], msg)
			}
		case <-time.After(1000 * time.Microsecond):
		}

		allBlock := false
		for !allBlock {
			allBlock = true
			for id, msgs := range msgQueue {
				if 0 == len(msgs) {
					continue
				}

				select {
				case pgroup[id].recvc <- msgs[0]:
					msgQueue[id] = msgQueue[id][1:]
					allBlock = false
				default:
				}
			}
		}
	}
}
