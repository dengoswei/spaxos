package spaxos

import (
	"bufio"
	"encoding/binary"
	"net"
	"time"
)

// simple network wrapper
func handleConnection(conn Conn, recvc chan pb.Message) {
	peerAddr := conn.RemoteAddr()
	if !n.validPeer(peerAddr) {
		return // reject invalid peer
	}

	reader := bufio.NewReader(conn)
	for {
		var pkglen uint64
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
			append(pkg, value)
		}

		assert(pkglen == len(pkg))
		msg := &pb.Message{}
		err = proto.Unmarshal(pkg, msg)
		if nil != err {
			return
		}

		// feed msg into node
		recvc <- msg
	}
}

func RunRecvMsg(ln Listener, recvc chan pb.Message) {
	for {
		conn, err := n.ln.Accept()
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

func doSendMsg(conn Conn, p peerInfo) error {
	for {
		msg := <-p.recvc
		assert(p.id == msg.Id)

		pkg, err := proto.Marshal(msg)
		if nil != err {
			continue
		}

		err := binary.Write(writer, binary.BigEndian, len(pkg))
		if nil != err {
			return err
		}

		for 0 != len(pkg) {
			writelen, err := p.conn.Write(pkg)
			if nil != err {
				return err
			}

			pkg = pkg[writelen:]
		}
	}
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

	var msgMap map[uint64][]pb.Message
	for {
		select {
		case m := <-sendc:
			append(msgMap[m.Id], m)
		case <-time.After(1 * time.MilliSecond):
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
