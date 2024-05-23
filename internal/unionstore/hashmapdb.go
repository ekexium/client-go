package unionstore

import (
	"errors"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type HashMapDB struct {
	Data                      map[string][]byte
	Flags                     map[string]kv.KeyFlags
	mutex                     sync.RWMutex
	dirty                     bool
	entrySizeLimit            uint64
	bufferSizeLimit           uint64
	memoryFootprintChangeHook func(uint64)
	skipMutex                 bool
}

func NewHashMapDB() *HashMapDB {
	return &HashMapDB{
		Data:  make(map[string][]byte),
		Flags: make(map[string]kv.KeyFlags),
	}
}

func (db *HashMapDB) Dirty() bool {
	return db.dirty
}

func (db *HashMapDB) Get(k []byte) ([]byte, error) {
	if !db.skipMutex {
		db.mutex.RLock()
		defer db.mutex.RUnlock()
	}
	value, ok := db.Data[string(k)]
	if !ok {
		return nil, tikverr.ErrNotExist
	}
	return value, nil
}

func (db *HashMapDB) GetFlags(k []byte) (kv.KeyFlags, error) {
	if !db.skipMutex {
		db.mutex.RLock()
		defer db.mutex.RUnlock()
	}
	flags, ok := db.Flags[string(k)]
	if !ok {
		return 0, tikverr.ErrNotExist
	}
	return flags, nil
}

func (db *HashMapDB) UpdateFlags(k []byte, ops ...kv.FlagsOp) {
	if !db.skipMutex {
		db.mutex.Lock()
		defer db.mutex.Unlock()
	}
	key := string(k)
	flags, _ := db.Flags[key]
	flags = kv.ApplyFlagsOps(flags, ops...)
	db.Flags[key] = flags
	db.dirty = true
}

func (db *HashMapDB) Set(k []byte, v []byte) error {
	return db.SetWithFlags(k, v)
}

func (db *HashMapDB) SetWithFlags(k []byte, v []byte, ops ...kv.FlagsOp) error {
	if !db.skipMutex {
		db.mutex.Lock()
		defer db.mutex.Unlock()
	}
	if uint64(len(k)+len(v)) > db.entrySizeLimit {
		return errors.New("entry size exceeds limit")
	}
	key := string(k)
	db.Data[key] = v
	flags := db.Flags[key]
	flags = kv.ApplyFlagsOps(flags, ops...)
	db.Flags[key] = flags
	db.dirty = true
	if db.memoryFootprintChangeHook != nil {
		db.memoryFootprintChangeHook(db.Mem())
	}
	return nil
}

func (db *HashMapDB) Delete(k []byte) error {
	return db.DeleteWithFlags(k)
}

func (db *HashMapDB) DeleteWithFlags(k []byte, ops ...kv.FlagsOp) error {
	return db.SetWithFlags(k, tombstone, ops...)
}

func (db *HashMapDB) Len() int {
	if !db.skipMutex {
		db.mutex.RLock()
		defer db.mutex.RUnlock()
	}
	return len(db.Data)
}

func (db *HashMapDB) Size() int {
	if !db.skipMutex {
		db.mutex.RLock()
		defer db.mutex.RUnlock()
	}
	// size := 0
	// for _, v := range db.data {
	// 	size += len(v)
	// }
	return db.Len()
}

func (db *HashMapDB) SetEntrySizeLimit(entrySizeLimit, bufferSizeLimit uint64) {
	db.entrySizeLimit = entrySizeLimit
	db.bufferSizeLimit = bufferSizeLimit
}

func (db *HashMapDB) SetMemoryFootprintChangeHook(hook func(uint64)) {
	db.memoryFootprintChangeHook = hook
}

func (db *HashMapDB) Mem() uint64 {
	if !db.skipMutex {
		db.mutex.RLock()
		defer db.mutex.RUnlock()
	}
	return uint64(db.Size())
}

func (db *HashMapDB) Staging() int {
	return 0
}

func (db *HashMapDB) Release(int) {
	// No-op for HashMapDB
}

func (db *HashMapDB) Cleanup(int) {
	// No-op for HashMapDB
}

func (db *HashMapDB) StageLen() uint {
	return 0
}

func (db *HashMapDB) SetSkipMutex(skip bool) {
	db.skipMutex = skip
}
