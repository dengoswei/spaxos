package spaxos

import (
	"fmt"
	"github.com/op/go-logging"
	"math/rand"
	"reflect"
	"runtime"
	"time"

	pb "spaxos/spaxospb"
)

var log = logging.MustGetLogger("spaxos")

var rd *rand.Rand

func assert(cond bool) {
	hassert(cond, "assert failed")
}

func hassert(cond bool, format string, args ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}

func LogDebug(format string, args ...interface{}) {
	log.Debug(format, args...)
}

func LogErr(format string, args ...interface{}) {
	log.Error(format, args...)
}

func MaxUint64(a, b uint64) uint64 {
	if a >= b {
		return a
	}
	return b
}

func MinUint64(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func RandUint64() uint64 {
	return uint64(rd.Int63())
}

func RandBool() bool {
	return 0 == rd.Int()%2
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]rune, n)
	rlen := len(letters)
	for i := range b {
		idx := rd.Intn(rlen)
		b[i] = letters[idx]
	}

	return string(b)
}

func RandByte(n int) []byte {
	return []byte(RandString(n))
}

func randPropItem() *pb.ProposeItem {
	pitem := &pb.ProposeItem{
		Reqid:  RandUint64(),
		Values: [][]byte{RandByte(10)}}
	return pitem
}

func randHardState() pb.HardState {
	acceptedNum := RandUint64()
	promisedNum := acceptedNum + RandUint64()
	hs := pb.HardState{
		Chosen:         false,
		Index:          RandUint64(),
		MaxProposedNum: RandUint64(),
		MaxPromisedNum: promisedNum,
		MaxAcceptedNum: acceptedNum,
		// TODO: test function stall on RandByte(100)!! ? why
		AcceptedValue: randPropItem(),
	}

	return hs
}

func randSpaxosInstance() *spaxosInstance {
	index := RandUint64()
	ins := newSpaxosInstance(index)

	ins.chosen = RandBool()
	ins.maxProposedNum = RandUint64()
	ins.promisedNum = RandUint64()
	ins.acceptedNum = MinUint64(ins.promisedNum, RandUint64())
	ins.acceptedValue = randPropItem()
	return ins
}

func randSpaxos() *spaxos {
	const groupCnt = uint64(9)
	id := uint64(rd.Intn(int(groupCnt))) + 1
	groups := make(map[uint64]bool, groupCnt)
	for idx := uint64(1); idx <= groupCnt; idx += 1 {
		groups[idx] = true
	}

	return newSpaxos(id, groups)
}

func randRspVotes(falseCnt, trueCnt uint64) map[uint64]bool {
	cnt := falseCnt + trueCnt
	rspVotes := make(map[uint64]bool, cnt)

	for id := uint64(1); id <= falseCnt; id += 1 {
		rspVotes[id] = false
	}

	for id := falseCnt + 1; id <= cnt; id += 1 {
		rspVotes[id] = true
	}

	return rspVotes
}

func randId(sp *spaxos, exclude bool) uint64 {
	for {
		newid := rd.Intn(len(sp.groups)) + 1
		if !exclude ||
			(exclude && uint64(newid) != sp.id) {
			return uint64(newid)
		}
	}
}

func randPropRsp(sp *spaxos, ins *spaxosInstance) pb.Message {
	randid := sp.id
	for id, _ := range sp.groups {
		if _, ok := ins.rspVotes[id]; !ok {
			randid = id
			break
		}
	}

	msg := pb.Message{
		Type: pb.MsgPropResp, Index: ins.index, Reject: false,
		From: randid, To: sp.id,
		Entry: pb.PaxosEntry{PropNum: ins.maxProposedNum}}
	return msg
}

func randAccptRsp(sp *spaxos, ins *spaxosInstance) pb.Message {
	randid := sp.id
	for id, _ := range sp.groups {
		if _, ok := ins.rspVotes[id]; !ok {
			randid = id
			break
		}
	}

	msg := pb.Message{
		Type: pb.MsgAccptResp, Index: ins.index, Reject: false,
		From: randid, To: sp.id,
		Entry: pb.PaxosEntry{
			PropNum: ins.maxProposedNum, Value: ins.proposingValue}}
	return msg
}

func randPropValue(cnt int) (uint64, [][]byte) {
	reqid := RandUint64()
	values := make([][]byte, cnt)
	for i := 0; i < cnt; i++ {
		values[i] = RandByte(10)
	}

	return reqid, values
}

func GetFunctionName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

func GetCurrentFuncName() string {
	pc, _, _, ok := runtime.Caller(1)
	assert(true == ok)

	return runtime.FuncForPC(pc).Name()
}

func printIndicate() {
	if nil == rd {
		s := rand.NewSource(time.Now().UnixNano())
		rd = rand.New(s)
	}

	pc, file, line, ok := runtime.Caller(1)
	assert(true == ok)
	fmt.Printf("[%s %s %d]\n", runtime.FuncForPC(pc).Name(), file, line)
}

func (ins *spaxosInstance) Equal(insB *spaxosInstance) bool {
	return ins.chosen == insB.chosen &&
		ins.index == insB.index &&
		ins.maxProposedNum == insB.maxProposedNum &&
		ins.promisedNum == insB.promisedNum &&
		ins.acceptedNum == insB.acceptedNum &&
		true == ins.acceptedValue.Equal(insB.acceptedValue)
}

func getMsg(
	c chan pb.Message,
	msgs []pb.Message) (chan pb.Message, pb.Message) {

	if nil != msgs && 0 < len(msgs) {
		return c, msgs[0]
	}

	return nil, pb.Message{}
}

func getMsgs(c chan []pb.Message, msgs []pb.Message) chan []pb.Message {
	if 0 == len(msgs) {
		return nil
	}
	return c
}

func generateTimeoutMsg(index, id, timestamp uint64) pb.Message {
	return pb.Message{Type: pb.MsgTimeOut,
		To: id, From: id, Index: index, Timestamp: timestamp}
}
