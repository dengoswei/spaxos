package spaxos

import (
	"fmt"
	"github.com/op/go-logging"
	"math/rand"
	"runtime"
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

func RandSpaxosInstance() *spaxosInstance {
	index := RandUint64()
	ins := newSpaxosInstance(index)

	ins.chosen = RandBool()
	ins.maxProposedNum = RandUint64()
	ins.promisedNum = RandUint64()
	ins.acceptedNum = MinUint64(ins.promisedNum, RandUint64())
	ins.acceptedValue = RandByte(rand.Intn(100))
	return ins
}

func RandSpaxos() *spaxos {
	const groupCnt = uint64(9)
	id := uint64(rand.Intn(int(groupCnt)))
	groups := make(map[uint64]bool, groupCnt)
	for idx := uint64(1); idx <= groupCnt; idx += 1 {
		groups[idx] = true
	}

	sp := &spaxos{id: id, groups: groups}
	return sp
}

func RandRspVotes(falseCnt, trueCnt uint64) map[uint64]bool {
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

func PrintIndicate() {
	pc, file, line, ok := runtime.Caller(1)
	assert(true == ok)
	fmt.Printf("[%s %s %d]\n", runtime.FuncForPC(pc).Name(), file, line)
}
