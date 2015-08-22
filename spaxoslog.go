package spaxos

import (
//	"encoding/json"
//	"io/ioutil"
)

type SpaxosLog struct {
	sp *spaxos
	db Storager

	minIndex uint64
	maxIndex uint64
}

func NewSpaxosLog(c *Config, db Storager) (*SpaxosLog, error) {
	assert(nil != c)
	sp, err := newSpaxos(c, db)
	if nil != err {
		return nil, err
	}

	assert(nil != sp)
	return &SpaxosLog{sp: sp, db: db,
		minIndex: sp.minIndex, maxIndex: sp.maxIndex}, nil
}

func (slog *SpaxosLog) Run(sw Switcher) {
	assert(nil != slog.sp)
	assert(nil != slog.db)
	assert(nil != sw)

	go slog.sp.runStateMachine()
	go slog.sp.runStorage(slog.db)
	go slog.sp.runSwitch(sw)
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
