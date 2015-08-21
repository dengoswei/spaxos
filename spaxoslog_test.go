package spaxos

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

const configFile = "./config.json"

func TestGroupEntry(t *testing.T) {
	printIndicate()

	entry := &GroupEntry{Id: 1, Ip: "127.0.0.1", Port: 10001}
	s, err := json.Marshal(entry)
	assert(nil == err)
	os.Stdout.Write(s)
	println()

	conststr := `{"id": 1, "ip": "127.0.0.1", "port": 10001}`
	newentry := &GroupEntry{}
	err = json.Unmarshal([]byte(conststr), newentry)
	assert(nil == err)
	fmt.Printf("%v\n", newentry)
}

func TestReadConfig(t *testing.T) {
	printIndicate()

	c, err := ReadConfig(configFile)
	if nil != err {
		fmt.Println("%s", err)
	}
	assert(nil == err)
	fmt.Printf("%d %v\n", len(c.Groups), c)

	assert(0 < c.Selfid)
	groups := c.GetGroupIds()
	assert(len(groups) == len(c.Groups))
	entry := c.GetEntry(c.Selfid)
	assert(c.Selfid == entry.Id)
}

func TestNewSpaxosLog(t *testing.T) {
	printIndicate()

	slog, err := NewSpaxosLog(configFile)
	assert(nil == err)
	assert(nil != slog)
}

func TestSpaxosLogRunAndStop(t *testing.T) {
	printIndicate()

	slog, err := NewSpaxosLog(configFile)
	assert(nil == err)
	assert(nil != slog)

	go slog.Run()
	time.Sleep(1 * time.Millisecond)
	slog.Stop()
}
