package spaxos

import (
	//	"encoding/json"
	//	"fmt"
	//	"os"
	"testing"
	"time"
)

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
