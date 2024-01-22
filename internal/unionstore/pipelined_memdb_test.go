// Copyright 2024 TiKV Authors
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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPipelinedFlushTrigger(t *testing.T) {
	avgKeySize := MinFlushSize / MinFlushKeys

	// block the flush goroutine for checking the flushing status.
	blockCh := make(chan struct{})
	defer close(blockCh)
	// Will not flush when keys number >= MinFlushKeys and size < MinFlushSize
	memdb := NewPipelinedMemDB(func(db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)-1) // (key + value) * MinFLushKeys < MinFlushKeys
		memdb.GetMemDB().Set(key, value)
		require.Nil(t, memdb.MayFlush())
		require.False(t, memdb.OnFlushing())
	}
	require.Equal(t, memdb.memBuffer.Len(), MinFlushKeys)
	require.Less(t, memdb.memBuffer.Size(), MinFlushSize)

	// Will not flush when keys number < MinFlushKeys and size >= MinFlushSize
	memdb = NewPipelinedMemDB(func(db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)+1) // (key + value) * (MinFLushKeys - 1) > MinFlushKeys
		memdb.GetMemDB().Set(key, value)
		require.Nil(t, memdb.MayFlush())
		require.False(t, memdb.OnFlushing())
	}
	require.Less(t, memdb.memBuffer.Len(), MinFlushKeys)
	require.Greater(t, memdb.memBuffer.Size(), MinFlushSize)

	// Flush when keys number >= MinFlushKeys and size >= MinFlushSize
	memdb = NewPipelinedMemDB(func(db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, avgKeySize-len(key)+1) // (key + value) * MinFLushKeys > MinFlushKeys
		memdb.GetMemDB().Set(key, value)
		require.Nil(t, memdb.MayFlush())
		if i == MinFlushKeys-1 {
			require.True(t, memdb.OnFlushing())
		} else {
			require.False(t, memdb.OnFlushing())
		}
	}
	require.Equal(t, memdb.memBuffer.Len(), 0)
	require.Equal(t, memdb.memBuffer.Size(), 0)
	// the flushing length and size should be added to the total length and size.
	require.Equal(t, memdb.Len(), MinFlushKeys)
	require.Equal(t, memdb.Size(), memdb.flushing.Size())
}

func TestPipelinedFlushSkip(t *testing.T) {
	blockCh := make(chan struct{})
	memdb := NewPipelinedMemDB(func(db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		memdb.GetMemDB().Set(key, value)
	}
	require.Nil(t, memdb.MayFlush())
	require.True(t, memdb.OnFlushing())
	require.Equal(t, memdb.memBuffer.Len(), 0)
	require.Equal(t, memdb.memBuffer.Size(), 0)
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		memdb.GetMemDB().Set(key, value)
	}
	require.Nil(t, memdb.MayFlush())
	// flush is skipped because there is an ongoing flush.
	require.Equal(t, memdb.memBuffer.Len(), MinFlushKeys)
	close(blockCh)
	for memdb.OnFlushing() {
	}
	// can flush when the ongoing flush is done.
	require.Nil(t, memdb.MayFlush())
	require.Equal(t, memdb.memBuffer.Len(), 0)
	require.Equal(t, memdb.len, 2*MinFlushKeys)
}

func TestPipelinedFlushBlock(t *testing.T) {
	blockCh := make(chan struct{})
	defer close(blockCh)
	memdb := NewPipelinedMemDB(func(db *MemDB) error {
		<-blockCh
		return nil
	})
	for i := 0; i < MinFlushKeys; i++ {
		key := []byte(strconv.Itoa(i))
		value := make([]byte, MinFlushSize/MinFlushKeys-len(key)+1)
		memdb.GetMemDB().Set(key, value)
	}
	require.Nil(t, memdb.MayFlush())
	require.True(t, memdb.OnFlushing())
	require.Equal(t, memdb.memBuffer.Len(), 0)
	require.Equal(t, memdb.memBuffer.Size(), 0)

	// When size of memdb is greater than MaxFlushSize, MayFlush will be blocked.
	for i := 0; i < MinFlushKeys-1; i++ {
		key := []byte(strconv.Itoa(MinFlushKeys + i))
		value := make([]byte, MaxFlushSize/(MinFlushKeys-1)-len(key)+1)
		memdb.GetMemDB().Set(key, value)
	}
	require.Greater(t, memdb.memBuffer.Size(), MaxFlushSize)
	mayFlushStart := make(chan struct{})
	oneSec := time.After(time.Second)
	go func() {
		memdb.MayFlush()
		close(mayFlushStart)
	}()
	select {
	case <-mayFlushStart:
		require.Fail(t, "MayFlush should be blocked")
	case <-oneSec:
	}
	require.True(t, memdb.OnFlushing())
	blockCh <- struct{}{} // first flush done
	<-mayFlushStart       // second flush start
	require.True(t, memdb.OnFlushing())
}
