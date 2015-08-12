package spaxos

import (
	"bytes"
	"fmt"
	"github.com/op/go-logging"
	"math/rand"
	"runtime"

	pb "spaxos/spaxospb"
)

var log = logging.MustGetLogger("spaxos")

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
	return uint64(rand.Int63())
}

func RandBool() bool {
	return 0 == rand.Int()%2
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}

func RandByte(n int) []byte {
	return []byte(RandString(n))
}

func randSpaxosInstance() *spaxosInstance {
	index := RandUint64()
	ins := newSpaxosInstance(index)

	ins.chosen = RandBool()
	ins.maxProposedNum = RandUint64()
	ins.promisedNum = RandUint64()
	ins.acceptedNum = MinUint64(ins.promisedNum, RandUint64())
	ins.acceptedValue = RandByte(rand.Intn(100))
	return ins
}

func randSpaxos() *spaxos {
	const groupCnt = uint64(9)
	id := uint64(rand.Intn(int(groupCnt))) + 1
	groups := make(map[uint64]bool, groupCnt)
	for idx := uint64(1); idx <= groupCnt; idx += 1 {
		groups[idx] = true
	}

	sp := &spaxos{id: id, groups: groups}
	return sp
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
		newid := rand.Intn(len(sp.groups)) + 1
		if !exclude ||
			(exclude && uint64(newid) != sp.id) {
			return uint64(newid)
		}
	}
}

func randPropResp(sp *spaxos, ins *spaxosInstance) pb.Message {
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

func printIndicate() {
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
		0 == bytes.Compare(ins.acceptedValue, insB.acceptedValue)
}
