package spaxos

import (
	"bytes"
	"flag"
	//	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	//	pb "spaxos/spaxospb"
	pb "github.com/dengoswei/spaxos/spaxospb"
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
		assert(3 == len(c.Groups))

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

func waitUntil(slog *SpaxosLog, beginIndex uint64,
	reqids []uint64, values [][][]byte, hostReqidMap map[uint64]uint64) {
	assert(0 < beginIndex)
	for {
		err := slog.Wait()
		assert(nil == err)
		cnt, err := slog.Get(beginIndex, reqids, values, hostReqidMap)
		if 0 != cnt {
			break
		}

		LogDebug("%s hostid %d slog.Get cnt %d",
			GetCurrentFuncName(), slog.sp.id, cnt)
	}
}

func busyWaitUntil(slog *SpaxosLog, beginIndex uint64,
	reqids []uint64, values [][][]byte, hostReqidMap map[uint64]uint64) {

	assert(0 < beginIndex)
	for {
		cnt, err := slog.Get(beginIndex, reqids, values, hostReqidMap)
		assert(nil == err)
		if 0 != cnt {
			break
		}

		LogDebug("%s hostid %d slog.Get cnt %d",
			GetCurrentFuncName(), slog.sp.id, cnt)
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSpaxosLogSimplePropose(t *testing.T) {
	printIndicate()
	assert(0 < len(testslog))
	assert(0 < len(testsw))

	slog := testslog[0]
	assert(nil != slog)

	// case 1
	{
		// Get on empty spaxos log
		reqids := make([]uint64, 1)
		values := make([][][]byte, 1)
		cnt, err := slog.Get(1, reqids, values, nil)
		assert(nil == err)
		assert(0 == cnt)
	}

	// case 2: sp 1 prop, all sp reach consensus
	{
		propItem := randPropItem()
		assert(nil != propItem)

		err := slog.Propose(propItem.Reqid, propItem.Values[0], false)
		assert(nil == err)

		reqids := make([]uint64, 1)
		values := make([][][]byte, 1)
		hostReqidMap := make(map[uint64]uint64)
		// TODO: fix busy wait;
		// should be more go style:
		// busyWaitUntil(slog, 1, reqids, values, hostReqidMap)
		waitUntil(slog, 1, reqids, values, hostReqidMap)

		assert(1 == len(reqids))
		assert(1 == len(values))
		assert(1 == len(hostReqidMap))

		assert(uint64(1) == hostReqidMap[propItem.Reqid])
		assert(reqids[0] == propItem.Reqid)
		assert(0 == bytes.Compare(propItem.Values[0], values[0][0]))

		// read chosen item from other peers
		for i := 1; i < len(testslog); i++ {
			oreqids := make([]uint64, 1)
			ovalues := make([][][]byte, 1)
			ohostReqidMap := make(map[uint64]uint64)

			assert(slog.sp.id != testslog[i].sp.id)
			// busyWaitUntil(testslog[i], 1, oreqids, ovalues, ohostReqidMap)
			waitUntil(testslog[i], 1, oreqids, ovalues, ohostReqidMap)

			assert(1 == len(oreqids))
			assert(1 == len(ovalues))
			assert(0 == len(ohostReqidMap))

			assert(oreqids[0] == propItem.Reqid)
			assert(0 == bytes.Compare(propItem.Values[0], ovalues[0][0]))
		}
	}

	// case 3: multi prop
	{
		propItems := []*pb.ProposeItem{}
		propReqids := make(map[uint64]bool)
		for i := 0; i < 2; i++ {
			propItem := randPropItem()
			propReqids[propItem.Reqid] = true
			propItems = append(propItems, propItem)

			err := testslog[i].Propose(propItem.Reqid, propItem.Values[0], false)
			assert(nil == err)
		}

		// wait one testslog[0]: slog
		hostSuccCnt := 0
		{
			reqids := make([]uint64, 1)
			values := make([][][]byte, 1)
			hostReqidMap := make(map[uint64]uint64)

			// busyWaitUntil(slog, 2, reqids, values, hostReqidMap)
			waitUntil(slog, 2, reqids, values, hostReqidMap)
			assert(1 == len(reqids))
			assert(1 == len(values))
			assert(1 == len(hostReqidMap))
			if reqids[0] == propItems[0].Reqid {
				hostSuccCnt++
				assert(0 == bytes.Compare(propItems[0].Values[0], values[0][0]))
			} else {
				assert(reqids[0] == propItems[1].Reqid)
				assert(0 == bytes.Compare(propItems[1].Values[0], values[0][0]))
			}
		}
		assert(0 <= hostSuccCnt)

		{
			for {
				oreqids := make([]uint64, 2)
				ovalues := make([][][]byte, 2)
				ohostReqidMap := make(map[uint64]uint64)

				// busyWaitUntil(testslog[1], 2, oreqids, ovalues, ohostReqidMap)
				waitUntil(testslog[1], 2, oreqids, ovalues, ohostReqidMap)
				assert(1 <= len(oreqids))
				assert(1 <= len(ovalues))
				assert(1 == len(ohostReqidMap))
				oindex, ok := ohostReqidMap[propItems[1].Reqid]
				if !ok {
					println("hostid %d prop not yet ready")
					continue
				}

				assert(0 < oindex)
				if oreqids[oindex-1] == propItems[1].Reqid {
					hostSuccCnt++
				}

				break
			}
		}
	}
}

func TestMain(m *testing.M) {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())
	var retcode int
	{
		setupSpaxosLogTest()
		retcode = m.Run()
		teardownSpaxosLogTest()
	}
	println("NumCPU", runtime.NumCPU())
	os.Exit(retcode)
}
