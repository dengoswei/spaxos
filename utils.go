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

func PrintIndicate() {
	pc, file, line, ok := runtime.Caller(1)
	assert(true == ok)
	fmt.Printf("[%s %s %d]\n", runtime.FuncForPC(pc).Name(), file, line)
}
