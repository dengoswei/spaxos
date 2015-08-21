package spaxos

import (
	"encoding/json"
	"io/ioutil"
)

type GroupEntry struct {
	Id   uint64 `json:id`
	Ip   string `json:ip`
	Port int    `json:port`
}

type Config struct {
	Selfid uint64       `json:selfid`
	Groups []GroupEntry `json:groups`
}

func (c *Config) GetGroupIds() map[uint64]bool {
	groups := make(map[uint64]bool)
	for _, entry := range c.Groups {
		assert(0 < entry.Id)
		groups[entry.Id] = true
	}
	return groups
}

func (c *Config) GetEntry(id uint64) GroupEntry {
	for _, entry := range c.Groups {
		if id == entry.Id {
			return entry
		}
	}
	assert(false)
	return GroupEntry{}
}

type SpaxosLog struct {
	sp *spaxos
	db Storager
	sw Switcher

	minIndex uint64
	maxIndex uint64
}

func ReadConfig(configFile string) (*Config, error) {

	content, err := ioutil.ReadFile(configFile)
	if nil != err {
		return nil, err
	}

	c := &Config{}
	err = json.Unmarshal(content, c)
	if nil != err {
		return nil, err
	}

	return c, nil
}

func NewSpaxosLog(c *Config, db Storager, sw Switcher) (*SpaxosLog, error) {

	slog := &SpaxosLog{db: db, sw: sw}

	// init sp
	{
		id := c.Selfid
		groups := c.GetGroupIds()

		sp := newSpaxos(id, groups)
		assert(nil != sp)

		err := sp.init(slog.db)
		if nil != err {
			return nil, err
		}

		slog.sp = sp
		slog.minIndex = sp.minIndex
		slog.maxIndex = sp.maxIndex
	}

	return slog, nil
}

func (slog *SpaxosLog) Run() {
	assert(nil != slog.sp)
	assert(nil != slog.db)
	assert(nil != slog.sw)

	go slog.sp.runStateMachine()
	go slog.sp.runStorage(slog.db)
	go slog.sp.runSwitch(slog.sw)
	slog.sp.runTick()
}

func (slog *SpaxosLog) Stop() {
	assert(nil != slog.sp)
	slog.sp.Stop()
}

func (slog *SpaxosLog) Propose(
	reqid uint64, data []byte, asMaster bool) error {

	return slog.sp.propose(reqid, data, asMaster)
}

func (slog *SpaxosLog) MultiPropose(
	reqid uint64, values [][]byte, asMaster bool) error {

	return slog.sp.multiPropose(reqid, values, asMaster)
}

func (slog *SpaxosLog) Get(
	beginIndex uint64, reqids []uint64, values [][][]byte) (int, error) {
	assert(0 < beginIndex)
	assert(len(reqids) == len(values))
	if 0 == len(reqids) {
		return 0, nil
	}

	if beginIndex > slog.minIndex {
		newMinIndex, newMaxIndex, err := slog.db.GetIndex()
		if nil != err {
			return 0, err
		}

		assert(newMinIndex >= slog.minIndex)
		assert(newMaxIndex >= slog.maxIndex)
		slog.maxIndex = newMaxIndex
		if newMinIndex == slog.minIndex {
			return 0, nil
		}
		slog.minIndex = newMinIndex
	}

	assert(beginIndex <= slog.minIndex)
	readCnt := MinUint64(
		slog.minIndex-beginIndex+1, uint64(len(reqids)))
	for i := uint64(0); i < readCnt; i++ {
		hs, err := slog.db.Get(beginIndex + i)
		if nil != err {
			// break
			if 0 == i {
				return 0, err
			}
			return int(i), nil
		}

		assert(true == hs.Chosen)
		assert(beginIndex+i == hs.Index)
		reqids[i] = hs.AcceptedValue.Reqid
		values[i] = hs.AcceptedValue.Values
	}

	return int(readCnt), nil
}
