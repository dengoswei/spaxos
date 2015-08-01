package spaxos

import (
	"errors"

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
	Chosen map[uint64]pb.ProposeValue

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

// stall util propose commit or timeout ?
func (n *node) Propose(uniqID uint64, data []byte) error {
	// try to propose

	pv := pb.ProposeValue{Id: uniqID, Value: data}
	value, err := pv.Marshal()
	if nil != err {
		return err
	}

	msg := pb.Message{
		Type: pb.MsgCliProp, Entry: pb.PaxosEntry{Value: value}}
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
			sp.Step(msg)

		case msg := <-n.recvc:
			if _, ok := sp.groups[msg.From]; ok {
				// msg from trust source
				sp.Step(msg)
			}

		case readyc <- rd:
			// TODO
			assert(nil == sp.prevHSS)
			sp.rebuild = nil
			sp.chosen = nil
			sp.prevHSS = sp.hss
			sp.hss = nil
			sp.msgs = nil
			advancec = n.advancec
		case ok := <-advancec:
			if !ok {
				// failed to save hss
				// => reload prevHSS into sp.hss
				sp.hss = append(sp.prevHSS, sp.hss...)
				sp.prevHSS = nil
			} else {
				sp.prevHSS = nil
			}

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
