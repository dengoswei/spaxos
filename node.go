package spaxos

import (
	"errors"

	proto "github.com/golang/protobuf/proto"
	pb "spaxos/spaxospb"
)

var (
	ErrStopped = errors.New("spaxos: stopped")
)

type Ready struct {

	// chosen value
	States []pb.HardState

	// entry to be safe to storage before message send
	Chosen map[uint64]pb.PropValue

	// msg wait to be send..
	Messages []pb.Message

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
	propc    chan []byte
	recvc    chan pb.Message
	readyc   chan Ready
	advancec chan struct{}
	tickc    chan struct{}
	done     chan struct{}
	stop     chan struct{}
}

// stall util propose commit or timeout ?
func (n *node) Propose(uniqID uint64, data []byte) error {
	// try to propose

	value, err := proto.Marshal(pb.PropValue{Id: uniqID, Value: data})
	if nil != err {
		return err
	}

	msg := pb.Message{Type: pb.MsgCliProp, PaxosEntry{Value: value}}
	select {
	case n.propc <- msg:
		return nil
	}
}

func (n *node) runPaxosInstance(sp *spaxos, data []byte) error {
	assert(nil != sp.currentEntry)

	index := sp.currentEntry.index
	p := sp.currentEntry.proposer
	a := sp.currentEntry.acceptor
	assert(0 < index)
	assert(nil != p)
	assert(nil != a)

	ecode := p.propose(data)
	if nil != ecode {
		return ecode
	}

	for {
		// 1. get
		state, msg, ecode := p.getMessages()
		if nil != ecode {
			return ecode
		}

		// safe state.
		select {}

	}
}

func (n *node) run(sp *spaxos) {
	var propc chan pb.Message
	var readyc chan Ready
	var advancec chan struct{}
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
			sp.Step(msg)

		case msg := <-n.recvc:
			if _, ok := sp.groups[msg.from]; ok {
				// msg from trust source
				sp.Step(msg)
			}

		case readyc <- rd:
			// TODO
			sp.rebuild = nil
			sp.chosen = nil
			sp.hss = nil
			sp.msgs = nil
			advancec = n.advancec
		case <-advancec:
			// TODO
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
	rd := Ready{
		States:       sp.hss,
		Chosen:       sp.chosen,
		Messages:     sp.msgs,
		RebuildIndex: sp.rebuild}
	return rd
}
