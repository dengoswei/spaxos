package spaxos

import (
	"testing"
	"time"

	pb "spaxos/spaxospb"
)

func TestNewSwitch(t *testing.T) {
	printIndicate()

	c := NewDefaultConfig()
	assert(nil != c)

	sw, err := NewSwitch(c)
	assert(nil == err)
	assert(nil != sw)
	assert(nil != sw.GetSendChan())
	assert(nil != sw.GetRecvChan())

	// test run and stop
	{
		go sw.Run()
		defer sw.Stop()
		time.Sleep(1 * time.Millisecond)
	}

	// simple send and recv
	{
		go sw.Run()
		defer sw.Stop()
		c2 := NewDefaultConfig()
		assert(nil != c2)
		c2.Selfid = 2
		sw2, err := NewSwitch(c2)
		assert(nil == err)
		assert(nil != sw2)
		go sw2.Run()
		defer sw2.Stop()

		testMsg := pb.Message{Type: pb.MsgBeat, From: sw.id, To: sw2.id}
		// send out test msg through sw
		sw.GetSendChan() <- testMsg
		LogDebug("%s submit test msg %v", GetCurrentFuncName(), testMsg)

		// expected recv test msg from sw2
		recvMsg := <-sw2.GetRecvChan()
		LogDebug("%s recv msg %v", GetCurrentFuncName(), recvMsg)
		assert(true == recvMsg.Equal(&testMsg))
	}
}
