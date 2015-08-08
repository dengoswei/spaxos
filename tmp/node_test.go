package spaxos

import (
	"math/rand"
	"strconv"
	"testing"
	//	"time"

	pb "spaxos/spaxospb"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func attach(n node, s *SNet) {
	for {
		msg := <-s.recvc
		n.recvc <- msg
	}
}

func TestAlone(t *testing.T) {
	n := newNode()
	groupsid := []uint64{1}
	peers := []string{":17001"}
	sp := newSpaxos(groupsid[0], groupsid, 0, 0)
	assert(nil != sp)
	go n.run(sp)

	s := newSNet(groupsid[0], groupsid, peers)
	assert(nil != s)
	err := s.RunRecvMsg()
	assert(nil == err)

	go s.RunSendMsg()
	go attach(n, s)

	pv := pb.ProposeValue{Id: 10001, Value: []byte("dengoswei")}
	value, err := pv.Marshal()
	assert(nil == err)
	err = n.Propose(value)
	assert(nil == err)

	i := 0

	var rd Ready
	for {
		rd = <-n.readyc
		if 0 != len(rd.Chosen) {
			break
		}
		i += 1
		if 0 != len(rd.States) {
			println("== store states")
		}

		if 0 != len(rd.Messages) {
			var msgs []pb.Message
			for _, msg := range rd.Messages {
				if 0 != msg.To {
					msgs = append(msgs, msg)
				} else {
					// broad cast
					for _, id := range groupsid {
						newmsg := msg
						newmsg.To = uint64(id)
						msgs = append(msgs, newmsg)
					}
				}
			}
			s.sendc <- msgs
		}

		n.Advance(true)
	}

	for index, value := range rd.Chosen {
		pv := pb.ProposeValue{}
		err = pv.Unmarshal(value)
		assert(nil == err)
		println("=> index", index, pv.Id, string(pv.Value))
	}
}

func printChosen(chosen map[uint64][]byte, chosenc chan []byte) {
	for index, value := range chosen {
		println("=> chosen: index ", index, len(value))
		chosenc <- value
	}
}

func runSpaxos(
	selfid uint64, groupsid []uint64, n node, s *SNet, chosenc chan []byte) {
	assert(nil != s)
	i := 0
	for {
		rd := <-n.readyc
		if 0 != len(rd.Chosen) {
			println("^^^ chosen", selfid, len(rd.Chosen))
			printChosen(rd.Chosen, chosenc)
		}

		i += 1
		if 0 != len(rd.States) {
			println("== store states", selfid)
		}

		if 0 != len(rd.Messages) {
			var msgs []pb.Message
			for _, msg := range rd.Messages {
				if 0 != msg.To {
					msgs = append(msgs, msg)
				} else {
					for _, id := range groupsid {
						newmsg := msg
						newmsg.To = uint64(id)
						msgs = append(msgs, newmsg)
					}
				}
			}

			s.sendc <- msgs
		}
		n.Advance(true)
	}
}

func TestBuild(t *testing.T) {
	// create nodeCnt node
	nodeCnt := 3

	ns := make([]node, nodeCnt)
	sps := make([]*spaxos, nodeCnt)

	groupsid := make([]uint64, nodeCnt)
	for i := 0; i < nodeCnt; i += 1 {
		groupsid[i] = uint64(i + 1)
	}

	for i := 0; i < nodeCnt; i += 1 {
		ns[i] = newNode()
		sps[i] = newSpaxos(groupsid[i], groupsid, 1, 1)
		assert(nil != sps[i])
		// start the node run
		go ns[i].run(sps[i])
	}

	snets := make([]*SNet, nodeCnt)
	addrs := make([]string, nodeCnt)
	for i := 0; i < nodeCnt; i += 1 {
		addrs[i] = ":" + strconv.Itoa(15000+i+1)
		println("addrs", addrs[i])
	}

	for i := 0; i < nodeCnt; i += 1 {
		selfid := groupsid[i]
		snets[i] = newSNet(selfid, groupsid, addrs)
		assert(nil != snets[i])
		err := snets[i].RunRecvMsg()
		assert(nil == err)
	}

	ccs := make([]chan []byte, nodeCnt)
	for i := 0; i < nodeCnt; i += 1 {
		go snets[i].RunSendMsg()
		go attach(ns[i], snets[i])
		ccs[i] = make(chan []byte)
		go runSpaxos(groupsid[i], groupsid, ns[i], snets[i], ccs[i])
	}

	for i := 0; i < 10; i += 1 {
		pv := pb.ProposeValue{
			Id: uint64(2000 + i), Value: []byte(randString(100))}
		value, err := pv.Marshal()
		assert(nil == err)

		pidx := i % len(ns)
		err = ns[pidx].Propose(value)
		assert(nil == err)

		cvalue := <-ccs[pidx]
		newpv := pb.ProposeValue{}
		err = newpv.Unmarshal(cvalue)
		assert(nil == err)
		assert(pv.Id == newpv.Id)
		assert(string(pv.Value) == string(newpv.Value))
		for idx := range groupsid {
			if pidx == idx {
				continue
			}

			cvalue = <-ccs[idx]
			newpv = pb.ProposeValue{}
			err = newpv.Unmarshal(cvalue)
			assert(nil == err)
			assert(pv.Id == newpv.Id)
			assert(string(pv.Value) == string(newpv.Value))
		}
	}
}
