package spaxos

import (
	//	"errors"

	pb "spaxos/spaxospb"
)

type spaxos struct {
	// id of myself := logid  id
	logid  uint32
	id     uint64
	groups map[uint64]bool

	// current state
	outMsgs       []pb.Message
	outHardStates []pb.HardState
	chosenItems   map[uint64]pb.HardState

	// index & spaxosInstance
	maxIndex uint64
	minIndex uint64
	insgroup map[uint64]*spaxosInstance

	// rebuild spaxosInstance
	rebuildList map[uint64][]pb.Message

	// communicate channel
	// attach: smt & db
	propc chan pb.Message

	// TODO:
	chindexc chan uint64
	chosenc  chan []pb.HardState

	// attach: smt & st
	storec chan storePackage

	// attach: st & nt
	sendc chan []pb.Message

	// attach: smt & nt
	recvc chan pb.Message

	// (0, minIndex) ins in allSps, if not,
	//    => re-build base ond disk hardstate
	// for index in (0, minIndex), will be a fifo queue
	// minIndex uint64
	// maxIndex uint64
	// pb.Message => index => ins need to re-build base on disk hardstate
	// fifoIndex []uint64
	// allSps    map[uint64]*spaxosInstance
	// mySps map[uint64]*spaxosInstance

	// msg handOn: wait for ins re-build;
	// handOn map[uint64][]pb.Message

	//	// index -> ins: need re-build
	//	rebuild []uint64 // rebuild index
	//	chosen  map[uint64][]byte
	//	hss     []pb.HardState
	//	msgs    []pb.Message
	//
	//	prevHSS []pb.HardState
}

func NewSpaxos(logid uint32, id uint64, groups map[uint64]bool) *spaxos {
	assert(0 != id)
	_, ok := groups[id]
	assert(true == ok)

	sp := &spaxos{logid: logid, id: id, groups: groups}

	sp.chosenItems = make(map[uint64]pb.HardState)
	sp.insgroup = make(map[uint64]*spaxosInstance)
	sp.rebuildList = make(map[uint64][]pb.Message)

	sp.propc = make(chan pb.Message)
	sp.chosenc = make(chan []pb.HardState)

	// TODO

	sp.storec = make(chan storePackage)
	sp.sendc = make(chan []pb.Message)
	sp.recvc = make(chan pb.Message)

	return sp
}

//func newSpaxos(
//	selfid uint64, groupsid []uint64, minIndex, maxIndex uint64) *spaxos {
//	groups := make(map[uint64]bool)
//	for _, id := range groupsid {
//		groups[id] = true
//	}
//
//	if _, ok := groups[selfid]; !ok {
//		// ERROR CASE
//		return nil
//	}
//
//	sp := &spaxos{
//		id:        selfid,
//		groups:    groups,
//		minIndex:  minIndex,
//		maxIndex:  maxIndex,
//		allSps:    make(map[uint64]*spaxosInstance),
//		mySps:     make(map[uint64]*spaxosInstance),
//		handOn:    make(map[uint64][]pb.Message),
//		currState: newSpaxosState()}
//	//		chosen:   make(map[uint64][]byte)}
//	assert(nil != sp)
//	return sp
//}
//

func (sp *spaxos) submitChosen(hs pb.HardState) {
	assert(true == hs.Chosen)
	assert(nil != sp.chosenItems)

	val, ok := sp.chosenItems[hs.Index]
	if !ok {
		sp.chosenItems[hs.Index] = hs
		return
	}

	// already chosen
	assert(true == val.Equal(&hs))
}

func (sp *spaxos) appendMsg(msg pb.Message) {
	assert(sp.id == msg.From)
	sp.outMsgs = append(sp.outMsgs, msg)
}

func (sp *spaxos) appendHardState(hs pb.HardState) {
	sp.outHardStates = append(sp.outHardStates, hs)
}

func (sp *spaxos) generateRebuildMsg(index uint64) pb.Message {
	return pb.Message{
		Type: pb.MsgInsRebuild, From: sp.id, To: sp.id, Index: index}
}

func (sp *spaxos) handOnMsg(msg pb.Message) {
	if _, ok := sp.rebuildList[msg.Index]; !ok {
		rebuildMsg := sp.generateRebuildMsg(msg.Index)
		sp.appendMsg(rebuildMsg)
	}

	sp.rebuildList[msg.Index] = append(sp.rebuildList[msg.Index], msg)
}

func (sp *spaxos) getNextProposeNum(prev, hint uint64) uint64 {
	assert(0 != sp.id)
	hint = MaxUint64(prev, hint)
	next := (hint + MaxNodeID - 1) / MaxNodeID * MaxNodeID
	return next + sp.id
}

func (sp *spaxos) asMajority(votes map[uint64]bool, cond bool) bool {
	assert(nil != votes)

	total := len(sp.groups)
	cnt := 0
	for _, b := range votes {
		if b == cond {
			cnt += 1
		}
	}
	return cnt > total/2
}

func (sp *spaxos) Run() {
	// runs state machine

	// TODO
}

// interface to db
// IMPORTANT:
// propValue := pb.ProposeValue{Reqid: reqid, Value: value}
// propItem := pb.ProposeItem{Values: append([]pb.ProposeValue, propValue)}
// data, err = propItem.Marshal()
// func (sp *spaxos) Propose(data []byte, asMaster bool) error {
func (sp *spaxos) Propose(data map[uint64][]byte, asMaster bool) error {
	msgType := pb.MsgCliProp
	if asMaster {
		// issue a master propose
		msgType = pb.MsgMCliProp
	}

	// data: may contain multiple <reqid, value>
	pitem := &pb.ProposeItem{}
	for reqid, value := range data {
		pitem.Values = append(pitem.Values,
			pb.ProposeValue{Reqid: reqid, Value: value})
	}
	assert(len(pitem.Values) == len(data))

	propMsg := pb.Message{
		Type: msgType, From: sp.id, To: sp.id,
		Entry: pb.PaxosEntry{Value: pitem}}
	select {
	case sp.propc <- propMsg:
		// TODO: add timeout ?
	}

	return nil
}

// IMPORTANT:
// map:
// - key: index
// - value:
//   item := pb.ProposeItem{}
//   err := item.Unmarshal(value)
//   for _, propValue := range item.values {
//       reqid := propValue.Reqid
//       value := propValue.Value
//   }
func (sp *spaxos) GetChosenValue() []pb.HardState {
	select {
	case chosengroup := <-sp.chosenc:
		// do not repeat !!!
		return chosengroup
		// TODO: add timeout ?
	}

	return nil
}

//func (sp *spaxos) GetChosenValueAt(index uint64) ([]byte, error) {
//	if 0 == index {
//		return nil, errors.New("INVALID INDEXI")
//	}
//
//	sp.chindexc <- index
//
//	select {
//	// TODO
//	case hs := <-sp.chvaluec:
//		assert(true == hs.Chosen)
//		assert(index == hs.Index)
//		return hs.AcceptedValue, nil
//		// TODO: timeout ?
//	}
//
//	// TODO
//	return nil, nil
//}

func (sp *spaxos) getChosen() (chan []pb.HardState, []pb.HardState) {
	if 0 == len(sp.chosenItems) {
		return nil, nil
	}

	cits := []pb.HardState{}
	for idx, hs := range sp.chosenItems {
		assert(idx == hs.Index)
		assert(true == hs.Chosen)
		assert(nil != hs.AcceptedValue)
		cits = append(cits, hs)
	}

	assert(len(sp.chosenItems) == len(cits))
	return sp.chosenc, cits
}

func (sp *spaxos) getStorePackage() (chan storePackage, storePackage) {
	if nil == sp.outMsgs && nil == sp.outHardStates {
		return nil, storePackage{}
	}

	return sp.storec, storePackage{
		outMsgs: sp.outMsgs, outHardStates: sp.outHardStates}
}

// State Machine Threaed
func (sp *spaxos) insertAndCheck(ins *spaxosInstance) {
	assert(0 != ins.index)
	_, ok := sp.insgroup[ins.index]
	assert(false == ok)
	sp.insgroup[ins.index] = ins
}

func (sp *spaxos) getSpaxosInstance(index uint64) *spaxosInstance {
	ins, ok := sp.insgroup[index]
	if ok {
		assert(nil != ins)
		return ins
	}

	assert(nil == ins)
	if index > sp.maxIndex {
		// new spaxos instance: cli prop
		ins = newSpaxosInstance(sp.logid, index)
		assert(nil != ins)
		sp.insertAndCheck(ins)

		LogDebug("inc sp.maxIndex from %d to %d", sp.maxIndex, index)
		sp.maxIndex = index
		return ins
	} else if index < sp.minIndex {
		// need rebuild
		return nil
	}

	// index >= sp.minIndex && index <= sp.maxIndex
	// propose by peers: simplely create a one
	ins = newSpaxosInstance(sp.logid, index)
	assert(nil != ins)
	sp.insertAndCheck(ins)
	return ins
}

func (sp *spaxos) stepNilSpaxosInstance(msg pb.Message) {
	// not yet available:
	// => hand on this msg & gen rebuild msg if not yet gen-ed
	if pb.MsgInsRebuildResp != msg.Type {
		sp.handOnMsg(msg)
		return
	}

	// rebuild ins
	ins := rebuildSpaxosInstance(msg.Hs)
	assert(nil != ins)
	assert(ins.index == msg.Index)
	// add ins into insgroups
	sp.insertAndCheck(ins)

	prevMsgs, ok := sp.rebuildList[ins.index]
	if ok {
		for _, prevMsg := range prevMsgs {
			sp.step(prevMsg)
		}
	}
}

func (sp *spaxos) step(msg pb.Message) {
	assert(0 != msg.Index)
	assert(sp.id == msg.To)

	// get ins by index
	ins := sp.getSpaxosInstance(msg.Index)
	if nil == ins {
		sp.stepNilSpaxosInstance(msg)
		return
	}

	switch msg.Type {
	case pb.MsgCliProp:
		fallthrough
	case pb.MsgMCliProp:
		assert(nil != msg.Entry.Value)
		ins.Propose(sp, msg.Entry.Value, pb.MsgMCliProp == msg.Type)

	default:
		ins.step(sp, msg)
	}
}

func (sp *spaxos) runStateMachine() {
	var propc chan pb.Message

	myindex := uint64(0)
	for {
		// select on propc only if myindex == 0
		if 0 == myindex {
			propc = sp.propc
		} else {
			propc = nil
		}

		// select on sp.chosenc only when abs needed
		chosenc, cits := sp.getChosen()

		// select on sp.storec only when abs needed
		storec, spkg := sp.getStorePackage()

		select {
		case propMsg := <-propc:
			assert(nil != propMsg.Entry.Value)
			propMsg.Index = sp.maxIndex + 1
			LogDebug("prop index %d propitem cnt %d firstitem len %d",
				propMsg.Index,
				len(propMsg.Entry.Value.Values),
				len(propMsg.Entry.Value.Values[0].Value))
			sp.step(propMsg)

		case msg := <-sp.recvc:
			assert(0 != msg.Index)
			// look up the coresponding spaxos instance
			// pass msg to spaxos instance
			assert(0 != msg.From)
			if sp.id == msg.To {
				sp.step(msg)
			}

		case chosenc <- cits:
			// success send out the chosen items
			// clean up the chosenItems state
			sp.chosenItems = make(map[uint64]pb.HardState)

		case storec <- spkg:
			// clean up state
			sp.outMsgs = nil
			sp.outHardStates = nil
		}
	}
}

func (sp *spaxos) generateBroadcastMsgs(msg pb.Message) []pb.Message {
	var msgs []pb.Message
	for id, _ := range sp.groups {
		if id != sp.id {
			newmsg := msg
			newmsg.To = id
			msgs = append(msgs, newmsg)
		}
	}

	return msgs
}

// Storage Thread
func (sp *spaxos) runStorage(db Storager) {
	assert(nil != db)

	var sendingMsgs []pb.Message
	for {

		select {
		case spkg := <-sp.storec:
			// deal with spkg
			outHardStates := spkg.outHardStates
			outMsgs := spkg.outMsgs
			assert(nil != outHardStates || nil != outMsgs)
			// store outhardStates first

			// shrink outHardStates
			{
				mapHardStates := make(map[uint64]pb.HardState)
				for _, hs := range outHardStates {
					mapHardStates[hs.Index] = hs
				}

				outHardStates = nil
				for _, hs := range mapHardStates {
					outHardStates = append(outHardStates, hs)
				}
			}

			dropMsgs := false
			if nil != outHardStates {
				err := db.Store(outHardStates)
				if nil != err {
					dropMsgs = true
				}
			}

			// deal with rebuild msg
			// generate broad-cast msg if needed
			for _, msg := range outMsgs {
				if pb.MsgInsRebuild == msg.Type {
					assert(0 < msg.Index)
					assert(sp.id == msg.From)
					assert(sp.id == msg.To)

					rspMsg := pb.Message{
						Type:   pb.MsgInsRebuildResp,
						Index:  msg.Index,
						From:   sp.id,
						To:     sp.id,
						Reject: false,
					}

					hs, err := db.Get(msg.Logid, msg.Index)
					if nil != err {
						rspMsg.Reject = true
					} else {
						rspMsg.Hs = hs
					}

					sendingMsgs = append(sendingMsgs, rspMsg)
				} else if !dropMsgs {
					if uint64(0) == msg.To {
						// broadcast but myself
						bmsgs := sp.generateBroadcastMsgs(msg)
						sendingMsgs = append(sendingMsgs, bmsgs...)
					} else {
						sendingMsgs = append(sendingMsgs, msg)
					}
				}
			}

		case sp.sendc <- sendingMsgs:
			// send out msgs
			sendingMsgs = nil
		}
	}
}

// Network Thread
func (sp *spaxos) runNetwork(net Networker) {
	var sendingMsgs []pb.Message
	var forwardingMsgs []pb.Message

	nrecvc := net.GetRecvChan()
	for {
		nsendc, smsg := getMsg(net.GetSendChan(), sendingMsgs)
		if nil != nsendc {
			assert(sp.id == smsg.From)
		}

		forwardc, fmsg := getMsg(sp.recvc, forwardingMsgs)
		if nil != forwardc {
			assert(sp.id == fmsg.To)
		}

		select {
		// collect msg from network recvc
		case msg := <-nrecvc:
			assert(0 < msg.Index)
			if msg.To == sp.id {
				forwardingMsgs = append(forwardingMsgs, msg)
			}

		// collect msgs from st
		case msgs := <-sp.sendc:
			for _, msg := range msgs {
				assert(0 < msg.Index)
				assert(sp.id == msg.From)
				if msg.To == sp.id {
					// forwarding msg
					forwardingMsgs = append(forwardingMsgs, msg)
				} else {
					sendingMsgs = append(sendingMsgs, msg)
				}
			}

		// sending msg only when needed
		case nsendc <- smsg:
			sendingMsgs = sendingMsgs[1:]

		// forwarding msg only when needed
		case forwardc <- fmsg:
			forwardingMsgs = forwardingMsgs[1:]
		}
	}
}
