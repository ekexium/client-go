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
	"sync"
	"sync/atomic"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
)

// PipelinedMemDB is a Store which contains
//   - a mutable buffer for write
//   - an immutable onflushing buffer for read
//   - like MemDB, PipelinedMemDB also CANNOT be used concurrently
type PipelinedMemDB struct {
	// This RWMutex only used to ensure memdbSnapGetter.Get will not race with
	// concurrent memdb.Set, memdb.SetWithFlags, memdb.Delete and memdb.UpdateFlags.
	sync.RWMutex
	onFlushing atomic.Bool
	errCh      chan error
	flushFunc  FlushFunc
	memBuffer  *MemDB
	flushing   *MemDB
	len, size  int // len and size records the total flushed and onflushing memdb.
}

const (
	MinFlushKeys = 10000
	MinFlushSize = 16 * 1024 * 1024  // 16MB
	MaxFlushSize = 128 * 1024 * 1024 // 128MB
)

type FlushFunc func(*MemDB) error

func NewPipelinedMemDB(flushFunc FlushFunc) *PipelinedMemDB {
	return &PipelinedMemDB{
		memBuffer: newMemDB(),
		errCh:     make(chan error, 1),
		flushFunc: flushFunc,
	}
}

// Dirty returns whether the pipelined buffer is mutated.
func (p *PipelinedMemDB) Dirty() bool {
	return p.memBuffer.Dirty() || p.flushing != nil
}

// Get the value by given key, it returns tikverr.ErrNotExist if not exist.
// The priority of the value is MemBuffer > flushing MemBuffer > Snapshot.
func (p *PipelinedMemDB) Get(k []byte) ([]byte, error) {
	var (
		memBuffer *MemDB
		flushing  *MemDB
	)
	if p.flushFunc != nil {
		memBuffer, flushing = p.memBuffer, p.flushing
	} else {
		memBuffer = p.memBuffer
	}

	v, err := memBuffer.Get(k)
	if flushing != nil && tikverr.IsErrNotFound(err) {
		v, err = flushing.Get(k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, tikverr.ErrNotExist
	}
	return v, nil
}

func (p *PipelinedMemDB) GetFlags(k []byte) (kv.KeyFlags, error) {
	var (
		memBuffer *MemDB
		flushing  *MemDB
	)
	if p.flushFunc != nil {
		memBuffer, flushing = p.memBuffer, p.flushing
	} else {
		memBuffer = p.memBuffer
	}

	f, err := memBuffer.GetFlags(k)
	if flushing != nil && tikverr.IsErrNotFound(err) {
		f, err = flushing.GetFlags(k)
	}
	if err != nil {
		return 0, err
	}
	return f, nil
}

// MayFlush is called during execution of a transaction, it does flush when there are enough keys and the ongoing flushing is done.
func (p *PipelinedMemDB) MayFlush() error {
	if p.flushFunc == nil {
		return nil
	}
	size := p.memBuffer.Size()
	if (p.memBuffer.Len() < MinFlushKeys || size < MinFlushSize) && size < MaxFlushSize {
		return nil
	}
	if !p.onFlushing.CompareAndSwap(false, true) && size < MaxFlushSize {
		return nil
	}
	if p.flushing != nil {
		if err := <-p.errCh; err != nil {
			return err
		}
		if size >= MaxFlushSize {
			p.onFlushing.Store(true)
		}
	}
	p.flushing = p.memBuffer
	p.len += p.flushing.Len()
	p.size += p.flushing.Size()
	p.memBuffer = newMemDB()
	go func() {
		p.errCh <- p.flushFunc(p.flushing)
		p.onFlushing.Store(false)
	}()
	return nil
}

// Flush will force flushing all the mutations, if there is an ongoing flush memdb, it will wait.
// this should be called before running commit protocol.
func (p *PipelinedMemDB) Flush() error {
	// wait until the ongoing flush is done.
	if p.flushing != nil {
		if err := <-p.errCh; err != nil {
			return err
		}
	}
	if p.memBuffer.Len() > 0 {
		p.len += p.memBuffer.Len()
		p.size += p.memBuffer.Size()
		return p.flushFunc(p.memBuffer)
	}
	return nil
}

// GetMemDB returns the mutable memdb for writing.
func (p *PipelinedMemDB) GetMemDB() *MemDB {
	return p.memBuffer
}

func (p *PipelinedMemDB) Len() int {
	return p.memBuffer.Len() + p.len
}

func (p *PipelinedMemDB) Size() int {
	return p.memBuffer.Size() + p.size
}

func (p *PipelinedMemDB) OnFlushing() bool {
	return p.onFlushing.Load()
}
