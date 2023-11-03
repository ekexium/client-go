// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/unionstore/memdb.go
//

// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unionstore

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
)

type store interface {
	SendReq(
		bo *retry.Backoffer, req *tikvrpc.Request, regionID locate.RegionVerID, timeout time.Duration,
	) (*tikvrpc.Response, error)
	GetRegionCache() *locate.RegionCache
	GetOracle() oracle.Oracle
}

type flagsAndValue struct {
	flags kv.KeyFlags
	value []byte
}

type TikvBuffer struct {
	sync.RWMutex

	store store

	startTs              uint64
	buffer               map[string]flagsAndValue
	secondaryPrimaryKeys map[string]struct{}
	primary              []byte
	spkCounter           int
}

func newTikvBuffer(store store) *TikvBuffer {
	return &TikvBuffer{
		store: store,
	}
}

/*

type Buffer interface {
	Staging() int
	Release(h int)
	Cleanup(h int)
	Checkpoint() *MemDBCheckpoint
	RevertToCheckpoint(cp *MemDBCheckpoint)
	Reset()
	DiscardValues()
	InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte))
	Get(key []byte) ([]byte, error)
	SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error)
	GetFlags(key []byte) (kv.KeyFlags, error)
	UpdateFlags(key []byte, ops ...kv.FlagsOp)
	Set(key []byte, value []byte) error
	SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error
	Delete(key []byte) error
	DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error
	GetKeyByHandle(handle MemKeyHandle) []byte
	GetValueByHandle(handle MemKeyHandle) ([]byte, bool)
	Len() int
	Size() int
	Dirty() bool
	RemoveFromBuffer(key []byte)
	SetMemoryFootprintChangeHook(hook func(uint64))
	Mem() uint64
}
*/

func (b *TikvBuffer) Staging() int {
	return 0
}

func (b *TikvBuffer) Release(h int) {

}

func (b *TikvBuffer) Cleanup(h int) {

}

func (b *TikvBuffer) Checkpoint() *MemDBCheckpoint {
	panic("unsupported")
}

func (b *TikvBuffer) RevertToCheckpoint(cp *MemDBCheckpoint) {
	panic("unsupported")
}

func (b *TikvBuffer) Reset() {
	panic("unsupported")
}

func (b *TikvBuffer) DiscardValues() {

}

func (b *TikvBuffer) InspectStage(handle int, f func([]byte, kv.KeyFlags, []byte)) {
	panic("unimplemented")
}

func (b *TikvBuffer) Get(key []byte) ([]byte, error) {
	if v, ok := b.buffer[string(key)]; ok {
		return v.value, nil
	}
	memBufferGetReq := tikvrpc.NewRequest(
		tikvrpc.CmdMemBufferGet, &kvrpcpb.MemBufferGetRequest{
			StartTs: b.startTs,
			Key:     key,
		},
	)
	bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
	region, err := b.store.GetRegionCache().LocateKey(bo.Clone(), key)
	if err != nil {
		return nil, err
	}
	resp, err := b.store.SendReq(bo.Clone(), memBufferGetReq, region.Region, 2*time.Second)
	if err != nil {
		return nil, err
	}
	memBufferGetResp := resp.Resp.(*kvrpcpb.MemBufferGetResponse)
	if memBufferGetResp.GetRegionError() != nil {
		return nil, errors.New("get error " + memBufferGetResp.GetRegionError().String())
	}
	if len(memBufferGetResp.GetError()) > 0 {
		return nil, errors.New("get error " + memBufferGetResp.GetError())
	}
	return memBufferGetResp.GetValue(), nil
}

func (b *TikvBuffer) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	panic("unsupported")
}

func (b *TikvBuffer) GetFlags(key []byte) (kv.KeyFlags, error) {
	return 0, errors.New("GetFlags is not supported")
}

func (b *TikvBuffer) UpdateFlags(key []byte, ops ...kv.FlagsOp) {

}

type kfv struct {
	key   []byte
	flag  uint32
	value []byte
}

func (b *TikvBuffer) maybeFlush() error {
	if len(b.buffer) < 2048 {
		return nil
	}
	return b.flush()
}

func (b *TikvBuffer) flush() (err error) {
	if len(b.primary) == 0 {
		return nil
	}

	if b.startTs == 0 {
		var err error
		b.startTs, err = b.store.GetOracle().GetTimestamp(nil, nil)
		if err != nil {
			return err
		}
	}

	// create a spk
	keys := make([][]byte, len(b.buffer))
	for k := range b.buffer {
		keys = append(keys, []byte(k))
	}
	spkValue, err := encode(keys)
	if err != nil {
		return err
	}
	b.spkCounter += 1
	spkKey := []byte("spk_" + string(b.startTs) + "_" + string(b.spkCounter))
	b.secondaryPrimaryKeys[string(spkKey)] = struct{}{}
	// put spk to buffer, commit together
	b.buffer[string(spkKey)] = flagsAndValue{
		flags: 0,
		value: spkValue,
	}
	defer func() {
		if err != nil {
			delete(b.buffer, string(spkKey))
			delete(b.secondaryPrimaryKeys, string(spkKey))
		}
	}()

	region2kvs := make(map[locate.RegionVerID][]kfv)
	regionCache := b.store.GetRegionCache()
	if regionCache == nil {
		return errors.New("region cache is nil")
	}
	bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
	for k, v := range b.buffer {
		region, err := regionCache.LocateKey(bo.Clone(), []byte(k))
		if err != nil {
			return err
		}
		region2kvs[region.Region] = append(
			region2kvs[region.Region], kfv{
				key:   []byte(k),
				flag:  uint32(v.flags),
				value: v.value,
			},
		)
	}

	for region, kfvs := range region2kvs {
		keys := make([][]byte, len(kfvs))
		flags := make([]uint32, len(kfvs))
		values := make([][]byte, len(kfvs))
		for i, kfv := range kfvs {
			keys[i] = kfv.key
			flags[i] = kfv.flag
			values[i] = kfv.value
		}
		req := tikvrpc.NewRequest(
			tikvrpc.CmdMemBufferSet, &kvrpcpb.MemBufferSetRequest{
				Keys:    keys,
				Flags:   flags,
				Values:  values,
				StartTs: b.startTs,
				Primary: b.primary,
			},
		)
		resp, err := b.store.SendReq(bo.Clone(), req, region, 10*time.Second)
		if err != nil {
			return err
		}
		setResp := resp.Resp.(*kvrpcpb.MemBufferSetResponse)
		if setResp.GetRegionError() != nil {
			return errors.New("set error " + setResp.GetRegionError().String())
		}
		if len(setResp.GetError()) > 0 {
			return errors.New("set error " + setResp.GetError())
		}
	}
	clear(b.buffer)
	return nil
}

func (b *TikvBuffer) Commit() error {
	b.flush()
	// commit primary
	commitTs, err := b.store.GetOracle().GetTimestamp(nil, nil)
	if err != nil {
		return err
	}
	commitReq := tikvrpc.NewRequest(
		tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
			CommitVersion: commitTs,
			StartVersion:  b.startTs,
			Keys: [][]byte{
				b.primary,
			},
		},
	)
	bo := retry.NewBackofferWithVars(context.Background(), 1000, nil)
	region, err := b.store.GetRegionCache().LocateKey(bo.Clone(), b.primary)
	resp, err := b.store.SendReq(
		retry.NewBackofferWithVars(context.Background(), 1000, nil),
		commitReq,
		region.Region,
		3*time.Second,
	)
	if err != nil {
		return err
	}
	commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
	if commitResp.GetRegionError() != nil {
		return errors.New("commit pk error " + commitResp.GetRegionError().String())
	}
	if commitResp.GetError() != nil {
		return errors.New("commit pk error " + commitResp.GetError().String())
	}

	// for each spk, read keys from its value, commit all keys
	for spk := range b.secondaryPrimaryKeys {
		spkValue, err := b.Get([]byte(spk))
		if err != nil {
			return err
		}
		keys := make([][]byte, 0)
		buffer := bytes.NewBuffer(spkValue)
		decoder := gob.NewDecoder(buffer)
		err = decoder.Decode(&keys)
		if err != nil {
			return err
		}

		region2keys := make(map[locate.RegionVerID][][]byte)
		for _, key := range keys {
			region, err := b.store.GetRegionCache().LocateKey(bo.Clone(), key)
			if err != nil {
				return err
			}
			region2keys[region.Region] = append(region2keys[region.Region], key)
		}
		for region, keys := range region2keys {
			commitReq := tikvrpc.NewRequest(
				tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
					CommitVersion: commitTs,
					StartVersion:  b.startTs,
					Keys:          keys,
				},
			)
			resp, err := b.store.SendReq(
				retry.NewBackofferWithVars(context.Background(), 1000, nil),
				commitReq,
				region,
				3*time.Second,
			)
			if err != nil {
				return err
			}
			commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
			if commitResp.GetRegionError() != nil {
				return errors.New("commit sk error" + commitResp.GetRegionError().String())
			}
			if commitResp.GetError() != nil {
				return errors.New("commit sk error " + commitResp.GetError().String())
			}
		}
	}

	return nil
}

func encode(keys [][]byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(keys)
	return buffer.Bytes(), err
}

func (b *TikvBuffer) Set(key []byte, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	b.buffer[string(key)] = flagsAndValue{
		flags: 0,
		value: value,
	}
	if len(b.primary) == 0 {
		b.primary = key
	}
	return b.maybeFlush()
}

func (b *TikvBuffer) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	b.buffer[string(key)] = flagsAndValue{
		flags: kv.ApplyFlagsOps(0, ops...),
		value: value,
	}
	return b.maybeFlush()
}

func (b *TikvBuffer) Delete(key []byte) error {
	return b.Set(key, tombstone)
}

func (b *TikvBuffer) DeleteWithFlags(key []byte, ops ...kv.FlagsOp) error {
	return b.SetWithFlags(key, tombstone, ops...)
}

func (b *TikvBuffer) GetKeyByHandle(handle MemKeyHandle) []byte {
	panic("unsupported")
}

func (b *TikvBuffer) GetValueByHandle(handle MemKeyHandle) ([]byte, bool) {
	panic("unsupported")
}

func (b *TikvBuffer) Len() int {
	panic("unsupported")
}

func (b *TikvBuffer) Size() int {
	panic("unsupported")
}

func (b *TikvBuffer) Dirty() bool {
	return true
}

func (b *TikvBuffer) RemoveFromBuffer(key []byte) {
	panic("unsupported")
}

func (b *TikvBuffer) SetMemoryFootprintChangeHook(hook func(uint64)) {

}

func (b *TikvBuffer) Mem() uint64 {
	return 0
}

func (b *TikvBuffer) SetEntrySizeLimit(entryLimit, bufferLimit uint64) {

}

func (b *TikvBuffer) Iter(k, upperBound []byte) (Iterator, error) {
	panic("unsupported")
}

func (b *TikvBuffer) IterReverse(k, lowerBound []byte) (Iterator, error) {
	panic("unsupported")
}

func (b *TikvBuffer) IterWithFlags(k []byte, upperBound []byte) *MemdbIterator {
	panic("unsupported")
}