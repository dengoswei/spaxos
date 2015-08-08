package spaxos

import (
	"errors"
	"log"

	pb "spaxos/spaxospb"
)

var (
	ErrStopped = errors.New("spaxos: stopped")
)

type Ready struct {

	// chosen value
	States []pb.HardState

	// msg wait to be send..
	Messages []pb.Message

	// entry to be safe to storage before message send
	Chosen map[uint64][]byte

	// spaxos instance need to rebuild
	RebuildIndex []uint64
}

func (rd *Ready) containsUpdates() bool {
	return 0 < len(rd.States) || 0 < len(rd.Chosen) ||
		0 < len(rd.Messages) || 0 < len(rd.RebuildIndex)
}

type Node interface {
	Propose(data []byte) error
	// return the chosen value
	Ready() <-chan Ready
}

type node struct {
	propc    chan pb.Message
	recvc    chan pb.Message
	readyc   chan Ready
	advancec chan bool
	tickc    chan struct{}
	done     chan struct{}
	stop     chan struct{}
}

func newNode() node {
	return node{
		propc:    make(chan pb.Message),
		recvc:    make(chan pb.Message),
		readyc:   make(chan Ready),
		advancec: make(chan bool),
		tickc:    make(chan struct{}),
		done:     make(chan struct{}),
		stop:     make(chan struct{})}
}

// stall util propose commit or timeout ?
func (n *node) Propose(value []byte) error {
	// try to propose
	msg := pb.Message{
		Type:  pb.MsgCliProp,
		Entry: pb.PaxosEntry{Value: value}}
	select {
	case n.propc <- msg:
		return nil
	}
}

func (n *node) run(sp *spaxos) {
	var propc chan pb.Message
	var readyc chan Ready
	var advancec chan bool
	var rd Ready

	for {
		if nil != advancec {
			readyc = nil
		} else {
			rd = newReady(sp)
			if rd.containsUpdates() {
				readyc = n.readyc
			} else {
				readyc = nil
			}
		}

		// make sure: prop 1 by 1
		if 0 == len(sp.mySps) {
			propc = n.propc
		} else {
			propc = nil
		}

		select {
		case msg := <-propc:
			msg.From = sp.id
			msg.To = sp.id
			sp.Step(msg)
			printMsg("prop:", msg)

		case msg := <-n.recvc:
			printMsg("recv:", msg)
			if _, ok := sp.groups[msg.From]; ok {
				// msg from trust source
				sp.Step(msg)
			}

		case readyc <- rd:
			// TODO
			assert(nil == sp.prevState)
			assert(nil != sp.currState)
			sp.prevState = sp.currState
			sp.currState = newSpaxosState()
			advancec = n.advancec
		case ok := <-advancec:
			if !ok {
				// failed to save hss
				// => reload prevHSS into sp.hss
				sp.currState.combine(sp.prevState)
			}

			sp.prevState = nil
			advancec = nil
		}
	}
}

func (n *node) Step(m pb.Message) error {
	select {
	case n.recvc <- m:
		return nil
	}
}

func newReady(sp *spaxos) Ready {
	assert(nil != sp.currState)
	rd := Ready{
		States:       sp.currState.hss,
		Chosen:       sp.currState.chosen,
		Messages:     sp.currState.msgs,
		RebuildIndex: sp.currState.rebuild}
	return rd
}

func (n *node) Advance(ret bool) {
	select {
	case n.advancec <- ret:
	}
}

func printMsg(hint string, msg pb.Message) {
	log.Printf("%s msg: To %d From %d Type %s Index %d",
		hint, msg.To, msg.From, pb.MessageType_name[int32(msg.Type)], msg.Index)
}
