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

const netType = "tcp"

type peerInfo struct {
	id          uint64
	addr        string
	sendc       chan pb.Message
	sendingMsgs []pb.Message
}

type SSwitch struct {
	id     uint64
	fsendc chan pb.Message
	frecvc chan pb.Message

	sendingMsgs []pb.Message
	recvingMsgs []pb.Message

	stop   chan struct{}
	done   chan struct{}
	nsendc chan pb.Message
	nrecvc chan pb.Message

	ln    net.Listener
	peers map[uint64]peerInfo
}

func NewSwitch(c *Config) (*SSwitch, error) {

	sw := &SSwitch{id: c.Selfid, peers: make(map[uint64]peerInfo)}
	// init sw.ln
	{
		entry := c.GetEntry(sw.id)
		assert(sw.id == entry.Id)
		ln, err := net.Listen(netType,
			fmt.Sprintf("%s:%d", entry.Ip, entry.Port))
		if nil != err {
			return nil, err
		}

		assert(nil != ln)
		sw.ln = ln
	}

	// init sw.peers
	{
		groups := c.GetGroupIds()
		for id, _ := range groups {
			if sw.id != id {
				entry := c.GetEntry(id)
				pinfo := peerInfo{id: id,
					addr:  fmt.Sprintf("%s:%d", entry.Ip, entry.Port),
					sendc: make(chan pb.Message)}
				sw.peers[id] = pinfo
			}
		}
		assert(len(groups) == len(sw.peers)+1)
	}

	sw.fsendc = make(chan pb.Message)
	sw.frecvc = make(chan pb.Message)
	sw.stop = make(chan struct{})
	sw.done = make(chan struct{})
	sw.nsendc = make(chan pb.Message)
	sw.nrecvc = make(chan pb.Message)

	return sw, nil
}

func (sw *SSwitch) GetSendChan() chan pb.Message {
	return sw.fsendc
}

func (sw *SSwitch) GetRecvChan() chan pb.Message {
	return sw.frecvc
}

func (sw *SSwitch) Stop() {
	assert(nil != sw.stop)
	select {
	case sw.stop <- struct{}{}:
	case <-sw.done:
		return
	}

	<-sw.done
	sw.ln.Close()
}

func (sw *SSwitch) Run() {
	go sw.runSendNetworkMsg()
	go sw.runRecvNetworkMsg()
	sw.runSwitch()
}

func (sw *SSwitch) handleNetworkConnection(conn net.Conn) {

	defer conn.Close()

	reader := bufio.NewReader(conn)
	assert(nil != reader)
	for {
		//		err := conn.SetReadDeadline(time.Now().Add(2 * time.Millisecond))
		//		if nil != err {
		//			LogErr("%s conn.SetReadDeadline conn %s err %s",
		//				GetCurrentFuncName(), conn, err)
		//			return
		//		}

		var pkglen int
		var err error
		{
			var totalLen uint32
			err = binary.Read(reader, binary.BigEndian, &totalLen)
			if nil != err {
				LogErr("%s binary.Read err %s", GetCurrentFuncName(), err)
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
				LogErr("%s binary.Read pkglen %d err %s",
					GetCurrentFuncName(), pkglen, err)
				return
			}

			assert(0 < readlen)
			pkg = append(pkg, value...)
		}

		assert(pkglen == len(pkg))
		msg := pb.Message{}
		err = msg.Unmarshal(pkg)
		if nil != err {
			LogErr("%s pb.Message.Unmarshal err %s",
				GetCurrentFuncName(), err)
			return
		}

		if _, ok := sw.peers[msg.From]; !ok || sw.id != msg.To {
			LogDebug("%s ignore msg %v",
				GetCurrentFuncName(), msg)
			continue
		}

		LogDebug("net switch hostid %d recv msg (type %s) %v",
			sw.id, pb.MessageType_name[int32(msg.Type)], msg)
		// feed msg into node
		select {
		case sw.nrecvc <- msg:
		case <-sw.done:
			return
		}
	}
}

func (sw *SSwitch) runRecvNetworkMsg() {
	assert(nil != sw.ln)

	for {
		conn, err := sw.ln.Accept()
		if nil != err {
			// log and ignore
			LogErr("%s Accept %s err %s",
				GetFunctionName(sw.runRecvNetworkMsg), conn, err)

			select {
			case <-sw.done:
				return
			default:
			}
			continue
		}

		assert(nil != conn)
		LogDebug("host %d Accept %s", sw.id, conn.RemoteAddr())
		go sw.handleNetworkConnection(conn)
	}
}

func sendOneMsg(conn net.Conn, pkg []byte) error {
	buf := new(bytes.Buffer)
	buf.Grow(4)
	{
		writelen := uint32(len(pkg))
		err := binary.Write(buf, binary.BigEndian, writelen)
		if nil != err {
			return err
		}
	}

	writelen, err := conn.Write(append(buf.Bytes(), pkg...))
	if nil != err {
		return err
	}

	assert(writelen == 4+len(pkg))
	return nil
}

func doSendMsg(conn net.Conn, sendc chan pb.Message) chan struct{} {
	assert(nil != conn)
	assert(nil != sendc)

	stop := make(chan struct{})
	go func() {
		defer close(stop)
		for {
			msg, ok := <-sendc
			if !ok {
				LogDebug("sendc been closed")
				return
			}

			pkg, err := msg.Marshal()
			if nil != err {
				LogErr("%s msg.Marshal msg %v err %s",
					GetCurrentFuncName(), msg, err)
				continue
			}

			conn.SetDeadline(time.Now().Add(2 * time.Millisecond))
			err = sendOneMsg(conn, pkg)
			if nil != err {
				LogErr("%s sendOneMsg pkglen %d err %s",
					GetCurrentFuncName(), len(pkg), err)
				return
			}
		}
	}()

	return stop
}

func (sw *SSwitch) handleSendNetworkMsg(pinfo peerInfo) {

	assert(nil != pinfo.sendc)
	for {
		conn, err := net.Dial(netType, pinfo.addr)
		if nil != err {
			LogErr("%s net.Dial addr %s err %s",
				GetCurrentFuncName(), pinfo.addr, err)
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-sw.done:
				return
			}
		}

		defer conn.Close()
		nsendc := make(chan pb.Message)
		defer close(nsendc)

		stop := doSendMsg(conn, nsendc)
		for {
			sendc, smsg := getMsg(nsendc, pinfo.sendingMsgs)
			select {
			case msg := <-pinfo.sendc:
				pinfo.sendingMsgs = append(pinfo.sendingMsgs, msg)
			case sendc <- smsg:
				pinfo.sendingMsgs = pinfo.sendingMsgs[1:]
			case <-sw.done:
				return
			case <-stop:
				LogDebug("%s doSendMsg stop", GetCurrentFuncName())
				break
			}
		}
	}
}

func (sw *SSwitch) runSendNetworkMsg() {
	assert(nil != sw.peers)

	for peerId, pinfo := range sw.peers {
		assert(peerId != sw.id)
		assert(peerId == pinfo.id)
		go sw.handleSendNetworkMsg(pinfo)
	}

	for {
		select {
		case smsg := <-sw.nsendc:
			LogDebug("net switch hostid %d send msg (type %s) %v",
				sw.id, pb.MessageType_name[int32(smsg.Type)], smsg)
			if pinfo, ok := sw.peers[smsg.To]; ok {
				assert(nil != pinfo.sendc)
				assert(smsg.To == pinfo.id)
				pinfo.sendc <- smsg
			} else {
				LogDebug("%s ignore smsg %v",
					GetCurrentFuncName(), smsg)
			}

		case <-sw.done:
			return
		}
	}
}

func (sw *SSwitch) runSwitch() {
	for {
		nsendc, smsg := getMsg(sw.nsendc, sw.sendingMsgs)
		frecvc, rmsg := getMsg(sw.frecvc, sw.recvingMsgs)

		select {
		case nsendc <- smsg:
			sw.sendingMsgs = sw.sendingMsgs[1:]
		case frecvc <- rmsg:
			sw.recvingMsgs = sw.recvingMsgs[1:]

		case msg := <-sw.fsendc:
			assert(sw.id == msg.From)
			sw.sendingMsgs = append(sw.sendingMsgs, msg)

		case msg := <-sw.nrecvc:
			if sw.id == msg.To {
				sw.recvingMsgs = append(sw.recvingMsgs, msg)
			} else {
				LogDebug("%s ignore recving msg %v",
					GetCurrentFuncName(), msg)
			}

		case <-sw.stop:
			close(sw.done)
			return
		}
	}
}
