package spaxos

import (
	"bufio"
	"encoding/binary"
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
		err := binary.Read(reader, binary.BigEndian, &pkglen)
		if nil != err {
			return // ERROR CASE
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
		err = msg.Unmarshal(pkg)
		if nil != err {
			return
		}

		// feed msg into node
		recvc <- msg
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

func sendMsg(writer *bufio.Writer, pkg []byte) error {
	err := binary.Write(writer, binary.BigEndian, len(pkg))
	if nil != err {
		return err
	}

	writelen, err := writer.Write(pkg)
	if nil != err {
		return err
	}

	assert(len(pkg) == writelen)
	return nil
}

func doSendMsg(conn net.Conn, p peerInfo) error {
	writer := bufio.NewWriter(conn)
	for {
		msg := <-p.recvc
		assert(p.id == msg.To)

		pkg, err := msg.Marshal()
		if nil != err {
			continue
		}

		err = sendMsg(writer, pkg)
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
		p := peerInfo{id: id, addr: addr}
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
