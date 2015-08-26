package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dengoswei/spaxos"
)

var LogDebug = spaxos.LogDebug

func assert(cond bool) {
	hassert(cond, "assert failed")
}

func hassert(cond bool, format string, args ...interface{}) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}

type simplesvr struct {
	slog *spaxos.SpaxosLog
	sw   *spaxos.SSwitch
}

func NewSimpleSvr(configFile string) (*simplesvr, error) {
	s := &simplesvr{}

	c, err := spaxos.ReadConfig(configFile)
	if nil != err {
		println(1)
		return nil, err
	}

	{
		db, err := spaxos.NewStorage(c)
		if nil != err {
			println(2)
			return nil, err
		}

		assert(nil != db)
		slog, err := spaxos.NewSpaxosLog(c, db)
		if nil != err {
			println(3)
			return nil, err
		}

		assert(nil != slog)
		s.slog = slog
	}

	{
		sw, err := spaxos.NewSwitch(c)
		if nil != err {
			return nil, err
		}

		assert(nil != sw)
		s.sw = sw
	}
	return s, nil
}

func waitUntil(slog *spaxos.SpaxosLog, beginIndex uint64,
	reqids []uint64, values [][][]byte, hostReqidMap map[uint64]uint64) error {
	assert(0 < beginIndex)
	timeout := time.After(3 * time.Second)
	for {
		err := slog.Wait()
		assert(nil == err)
		cnt, err := slog.Get(beginIndex, reqids, values, hostReqidMap)
		if 0 != cnt {
			break
		}

		select {
		case <-timeout:
			LogDebug("TIMEOUT")
			return errors.New("TIMEOUT")
		default:
		}
	}
	return nil
}

func (s *simplesvr) Run(addr string) {
	assert(nil != s.sw)
	assert(nil != s.slog)
	go s.sw.Run()
	go s.slog.Run(s.sw)

	ln, err := net.Listen("tcp", addr)
	if nil != err {
		fmt.Printf("s Run listen %s err %s", addr, err)
		return
	}

	assert(nil != ln)
	defer ln.Close()

	beginIndex := uint64(1)
	for {
		assert(0 < beginIndex)
		conn, err := ln.Accept()
		if nil != err {
			LogDebug("s %s Accept %s err %s", addr, conn, err)
			continue
		}
		defer conn.Close()
		LogDebug("s %s Accept %s", addr, conn.RemoteAddr())

		reader := bufio.NewReader(conn)
		assert(nil != reader)

		cmd, err := reader.ReadString('\n')
		if nil != err {
			LogDebug("s %s ReadString err %s", addr, err)
			continue
		}

		tokens := strings.Split(cmd, " ")
		if 3 != len(tokens) {
			LogDebug("s %s invalid cmd %s", addr, cmd)
			continue
		}

		assert("PROP" == tokens[0])
		reqid, err := strconv.ParseUint(tokens[1], 10, 64)
		assert(nil == err)
		assert(0 < reqid)

		value := []byte(tokens[2])
		assert(0 < len(value))

		err = s.slog.Propose(reqid, value, false)
		assert(nil == err)

		for {
			reqids := make([]uint64, 1)
			values := make([][][]byte, 1)
			hostReqidMap := make(map[uint64]uint64)

			err = waitUntil(s.slog, beginIndex, reqids, values, hostReqidMap)
			if nil != err {
				LogDebug("waitUntil err %s", err)
				break
			}
			assert(1 == len(reqids))
			if 0 != len(hostReqidMap) {
				index, ok := hostReqidMap[reqid]
				assert(true == ok)
				assert(beginIndex == index)

				if reqids[0] != reqid {
					LogDebug("%s propose failed reqid %d reqids[0] %d", addr, reqid, reqids[0])
				} else {
					LogDebug("%s propose succ reqids[0] %d values %s", addr, reqids[0], values[0])
				}

				break
			}
			LogDebug("%s inc beginIndex to %d", addr, beginIndex)
			beginIndex++
		}
	}
}

func main() {
	println(os.Args[0], "config ip:port")
	assert(3 == len(os.Args))

	configFile := os.Args[1]
	assert(0 < len(configFile))
	s, err := NewSimpleSvr(configFile)
	if nil != err {
		fmt.Printf("NewSimpleSvr %s err %s", configFile, err)
		return
	}

	assert(nil != s)
	addr := os.Args[2]
	assert(0 < len(addr))
	s.Run(addr)
}
