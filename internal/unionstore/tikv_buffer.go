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
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

type flagsAndValue struct {
	flags kv.KeyFlags
	value []byte
}

type TikvBuffer struct {
	sync.RWMutex
	// hashmap of key to (flags and value)
	m map[string]flagsAndValue
}

func newTikvBuffer() *TikvBuffer {
	return &TikvBuffer{
		m: make(map[string]flagsAndValue),
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
	if v, ok := b.m[string(key)]; ok {
		return v.value, nil
	}
	return nil, tikverr.ErrNotExist
}

func (b *TikvBuffer) SelectValueHistory(key []byte, predicate func(value []byte) bool) ([]byte, error) {
	panic("unsupported")
}

func (b *TikvBuffer) GetFlags(key []byte) (kv.KeyFlags, error) {
	if v, ok := b.m[string(key)]; ok {
		return v.flags, nil
	}
	return 0, tikverr.ErrNotExist
}

func (b *TikvBuffer) UpdateFlags(key []byte, ops ...kv.FlagsOp) {
	if v, ok := b.m[string(key)]; ok {
		newFlags := kv.ApplyFlagsOps(v.flags, ops...)
		b.m[string(key)] = flagsAndValue{
			flags: newFlags,
			value: v.value,
		}
	} else {
		b.m[string(key)] = flagsAndValue{
			flags: kv.ApplyFlagsOps(0, ops...),
			value: nil,
		}
	}
}

func (b *TikvBuffer) Set(key []byte, value []byte) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	if v, ok := b.m[string(key)]; ok {
		b.m[string(key)] = flagsAndValue{
			flags: v.flags,
			value: value,
		}
	} else {
		b.m[string(key)] = flagsAndValue{
			flags: 0,
			value: value,
		}
	}
	return nil
}

func (b *TikvBuffer) SetWithFlags(key []byte, value []byte, ops ...kv.FlagsOp) error {
	if len(value) == 0 {
		return tikverr.ErrCannotSetNilValue
	}
	if v, ok := b.m[string(key)]; ok {
		newFlags := kv.ApplyFlagsOps(v.flags, ops...)
		b.m[string(key)] = flagsAndValue{
			flags: newFlags,
			value: value,
		}
	} else {
		b.m[string(key)] = flagsAndValue{
			flags: kv.ApplyFlagsOps(0, ops...),
			value: value,
		}
	}
	return nil
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