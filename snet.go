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

// simple network wrapper
func handleConnection(conn net.Conn, recvc chan pb.Message) {
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

		// feed msg into node
		select {
		case recvc <- msg:
		}
	}
}

func RunRecvMsg(ln net.Listener, recvc chan pb.Message) {
	for {
		conn, err := ln.Accept()
		if nil != err {
			continue
		}

		go handleConnection(conn, recvc)
	}
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

		doSendMsg(conn, p)
	}
}

func RunSendMsg(addrbook map[uint64]string, sendc chan pb.Message) {

	var pgroup map[uint64]peerInfo
	for id, addr := range addrbook {
		p := peerInfo{
			id: id, addr: addr, recvc: make(chan pb.Message)}
		pgroup[id] = p
		go handleSendMsg(p)
	}

	type pbs []pb.Message
	var msgMap map[uint64][]pb.Message
	for {
		select {
		case m := <-sendc:
			msgMap[m.To] = append(msgMap[m.To], m)
		case <-time.After(1 * time.Millisecond):
		}

		// loop over pgroup: try to send out msg
		for id, p := range pgroup {
			if msgs, ok := msgMap[id]; ok {
				if 0 < len(msgs) {
					msg := msgs[0]
					select {
					case p.recvc <- msg:
						msgMap[id] = msgs[1:]
					default:
					}
				}
			}
		}
	}
}
