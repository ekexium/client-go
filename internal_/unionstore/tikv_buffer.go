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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal_/locate"
	"github.com/tikv/client-go/v2/internal_/logutil"
	"github.com/tikv/client-go/v2/internal_/retry"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
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

func newTikvBuffer(startTS uint64, store store) *TikvBuffer {
	return &TikvBuffer{
		store:                store,
		startTs:              startTS,
		buffer:               make(map[string]flagsAndValue),
		secondaryPrimaryKeys: make(map[string]struct{}),
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
		// println("get from memory")
		return v.value, nil
	}
	return b.getFromTiKV(key)
}

func (b *TikvBuffer) getFromTiKV(key []byte) ([]byte, error) {
	var v []byte
	f := func() error {
		var err error
		v, err = b.doGetFromTiKV(key)
		return err
	}
	return v, withRetry(f)
}

func (b *TikvBuffer) doGetFromTiKV(key []byte) ([]byte, error) {
	memBufferGetReq := tikvrpc.NewRequest(
		tikvrpc.CmdMemBufferGet, &kvrpcpb.MemBufferGetRequest{
			StartTs: b.startTs,
			Key:     key,
		},
	)
	bo := retry.NewBackofferWithVars(context.Background(), 1000000, nil)
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
	return b.Flush()
}

func (b *TikvBuffer) Flush() (err error) {
	return withRetry(b.doFlush)
}

func withRetry(f func() error) (err error) {
	tryTimes := 0
	for {
		tryTimes++
		err = f()
		if err == nil {
			return nil
		}
		if tryTimes > 20 {
			logutil.BgLogger().Error("retry fails", zap.Error(err))
			return err
		} else {
			logutil.BgLogger().Warn("retrying", zap.Error(err))
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (b *TikvBuffer) doFlush() (err error) {
	if len(b.primary) == 0 {
		return nil
	}

	if b.startTs == 0 {
		return errors.New("start ts can't be 0")
	}

	if len(b.buffer) == 0 {
		return nil
	}

	// create a spk
	keys := make([][]byte, 0, len(b.buffer))
	for k := range b.buffer {
		keys = append(keys, []byte(k))
	}
	spkValue, err := encode(keys)
	if err != nil {
		return err
	}
	// fmt.Printf("encoded, keys=%v, buffer=%v\n", keys, b.buffer)
	b.spkCounter += 1
	spkKey := []byte(fmt.Sprintf("spk_%v_%v", b.startTs, b.spkCounter))
	b.secondaryPrimaryKeys[string(spkKey)] = struct{}{}
	// put spk to buffer, commit together
	b.buffer[string(spkKey)] = flagsAndValue{
		flags: 0,
		value: spkValue,
	}
	// fmt.Printf("setting SPK %v\n", spkKey)
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
	bo := retry.NewBackofferWithVars(context.Background(), 1000000, nil)
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
		// println("mem_buffer_set sent")
		if err != nil {
			return err
		}
		if resp == nil {
			// println(resp)
			return errors.New("resp is nil")
		}
		if resp.Resp == nil {
			return errors.New("resp.Resp is nil")
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
	b.Flush()
	// commit primary
	commitTs, err := b.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	if err != nil {
		return err
	}
	err = b.commitPK(commitTs)
	if err != nil {
		return err
	}

	err = b.commitSK(commitTs)
	if err != nil {
		return err
	}

	return nil
}

func (b *TikvBuffer) commitSK(commitTs uint64) error {
	f := func() error {
		return b.doCommitSK(commitTs)
	}
	return withRetry(f)
}

func (b *TikvBuffer) doCommitSK(commitTs uint64) error {
	bo := retry.NewBackofferWithVars(context.Background(), 1000000, nil)
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
				retry.NewBackofferWithVars(context.Background(), 1000000, nil),
				commitReq,
				region,
				3*time.Second,
			)
			if err != nil {
				return err
			}
			commitResp := resp.Resp.(*kvrpcpb.CommitResponse)
			if commitResp.GetRegionError() != nil {
				return errors.New("commit sk error, " + commitResp.GetRegionError().String())
			}
			if commitResp.GetError() != nil {
				return errors.New("commit sk error, " + commitResp.GetError().String())
			}
		}
	}
	return nil
}

func (b *TikvBuffer) commitPK(commitTs uint64) error {
	f := func() error {
		return b.doCommitPK(commitTs)
	}
	return withRetry(f)
}

func (b *TikvBuffer) doCommitPK(commitTs uint64) error {
	commitReq := tikvrpc.NewRequest(
		tikvrpc.CmdCommit, &kvrpcpb.CommitRequest{
			CommitVersion: commitTs,
			StartVersion:  b.startTs,
			Keys: [][]byte{
				b.primary,
			},
		},
	)
	bo := retry.NewBackofferWithVars(context.Background(), 1000000, nil)
	region, err := b.store.GetRegionCache().LocateKey(bo.Clone(), b.primary)
	if err != nil {
		return err
	}
	resp, err := b.store.SendReq(
		retry.NewBackofferWithVars(context.Background(), 1000000, nil),
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
	return nil
}

func encode(keys [][]byte) ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(keys)
	return buffer.Bytes(), err
}

func (b *TikvBuffer) Set(key []byte, value []byte) error {
	// fmt.Printf("set key %v\n", key)
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
	// fmt.Printf("after set, buffer = %v\n", b.buffer)
	// defer func() {
	// fmt.Printf("after set maybe flush, buffer = %v\n", b.buffer)
	// }()
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
	// fmt.Printf("after set with flags, buffer = %v\n", b.buffer)
	// defer func() {
	// fmt.Printf("after set with flags maybe flush, buffer = %v\n", b.buffer)
	// }()
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
