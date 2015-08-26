package spaxos

import (
	"fmt"
	"path"
	"strconv"

	"github.com/golang/leveldb"
	ldb "github.com/golang/leveldb/db"
	// pb "spaxos/spaxospb"
	pb "github.com/dengoswei/spaxos/spaxospb"
)

var keyMinIndex = []byte("min_index")
var keyMaxIndex = []byte("max_index")

type SStorage struct {
	db *leveldb.DB
}

func makeDataKey(index uint64) []byte {
	skey := fmt.Sprintf("index_%d", index)
	assert(0 < len(skey))

	return []byte(skey)
}

func NewStorage(c *Config) (*SStorage, error) {
	assert(nil != c)

	// TODO: using leveldb/memfs ?
	db, err := leveldb.Open(
		path.Join(c.Path, strconv.FormatUint(c.Selfid, 10)), nil)
	if nil != err {
		return nil, err
	}

	store := &SStorage{db: db}
	return store, nil
}

func (store *SStorage) Store(data []pb.HardState) error {
	assert(nil != store.db)

	// group into one batch
	var batch leveldb.Batch
	for _, hs := range data {
		key := makeDataKey(hs.Index)
		assert(0 < len(key))
		val, err := hs.Marshal()
		if nil != err {
			return err
		}

		batch.Set(key, val)
	}

	// store using default option
	return store.db.Apply(batch, nil)
}

func (store *SStorage) Get(index uint64) (pb.HardState, error) {
	assert(nil != store.db)
	key := makeDataKey(index)
	assert(0 < len(key))

	val, err := store.db.Get(key, nil)
	if nil != err {
		return pb.HardState{}, err
	}

	hs := pb.HardState{}
	err = hs.Unmarshal(val)
	if nil != err {
		return pb.HardState{}, err
	}

	return hs, nil
}

func (store *SStorage) setIndex(key []byte, index uint64) error {
	assert(nil != store.db)
	assert(nil != key)

	hs := pb.HardState{Index: index}
	val, err := hs.Marshal()
	if nil != err {
		return err
	}

	return store.db.Set(key, val, nil)
}

func (store *SStorage) getIndex(key []byte) (uint64, error) {
	assert(nil != store.db)
	assert(nil != key)

	val, err := store.db.Get(key, nil)
	if nil != err {
		if ldb.ErrNotFound == err {
			return 0, nil
		}
		return 0, err
	}

	hs := pb.HardState{}
	err = hs.Unmarshal(val)
	if nil != err {
		return 0, err
	}

	return hs.Index, nil
}

func (store *SStorage) SetIndex(minIndex, maxIndex uint64) error {
	err := store.setIndex(keyMinIndex, minIndex)
	if nil != err {
		return err
	}

	return store.setIndex(keyMaxIndex, maxIndex)
}

func (store *SStorage) GetIndex() (uint64, uint64, error) {
	minIndex, err := store.getIndex(keyMinIndex)
	if nil != err {
		return 0, 0, err
	}

	maxIndex, err := store.getIndex(keyMaxIndex)
	if nil != err {
		return 0, 0, err
	}

	return minIndex, maxIndex, nil
}

func (store *SStorage) Close() error {
	assert(nil != store.db)
	return store.db.Close()
}
