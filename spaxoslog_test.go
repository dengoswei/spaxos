package spaxos

import (
	"flag"
	"os"
	"testing"
	"time"
)

var testslog []*SpaxosLog
var testsw []*SSwitch

func setupSpaxosLogTest() {
	printIndicate()
	assert(nil == testslog)
	assert(nil == testsw)

	for id := uint64(1); id <= uint64(3); id++ {
		c := NewDefaultConfig()
		assert(nil != c)
		c.Selfid = id
		assert(int(id) <= len(c.Groups))

		db := NewFakeStorage()
		assert(nil != db)

		slog, err := NewSpaxosLog(c, db)
		assert(nil == err)
		assert(nil != slog)

		sw, err := NewSwitch(c)
		assert(nil == err)
		assert(nil != sw)

		go slog.Run(sw)
		go sw.Run()

		testslog = append(testslog, slog)
		testsw = append(testsw, sw)
	}
}

func teardownSpaxosLogTest() {
	assert(nil != testslog)
	assert(nil != testsw)

	assert(len(testslog) == len(testsw))
	for i := 0; i < len(testslog); i++ {
		testslog[i].Stop()
		testsw[i].Stop()
	}
}

func TestNewSpaxosLog(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)
	db := NewFakeStorage()
	assert(nil != db)
	slog, err := NewSpaxosLog(c, db)
	assert(nil == err)
	assert(nil != slog)
}

func TestSpaxosLogRunAndStop(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)
	db := NewFakeStorage()
	assert(nil != db)
	slog, err := NewSpaxosLog(c, db)
	assert(nil == err)
	assert(nil != slog)

	sw := NewFakeSwitch(c.Selfid)
	assert(nil != sw)
	go slog.Run(sw)
	time.Sleep(1 * time.Millisecond)
	slog.Stop()
}

func TestMain(m *testing.M) {
	flag.Parse()

	var retcode int
	{
		setupSpaxosLogTest()
		retcode = m.Run()
		teardownSpaxosLogTest()
	}
	os.Exit(retcode)
}
