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

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tikv/client-go/v2/internal_/logutil"
	"github.com/tikv/client-go/v2/internal_/unionstore"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

var (
	client *txnkv.Client
	pdAddr = flag.String("pd", "127.0.0.1:2379", "pd address")
)

// Init initializes information.
func initStore() {
	var err error
	client, err = txnkv.NewClient([]string{*pdAddr})
	if err != nil {
		panic(err)
	}
}

func main() {
	value := make([]byte, 1024)
	value[0] = 42
	// value := []byte{42}
	maxn := 10_000_000
	prefix := "exp29_"
	largeTxn := false

	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-pd", pdAddr)
	}
	flag.Parse()
	initStore()

	option := func(opt *transaction.TxnOptions) { opt.LargeTxn = largeTxn }
	txn, err := client.Begin(option)
	if err != nil {
		panic(err)
	}
	if largeTxn {
		goLargeTxn(txn, maxn, prefix, value)
	} else {
		goNormalTxn(txn, maxn, prefix, value)
	}

	readTxn, err := client.Begin()
	logutil.BgLogger().Info("read ts", zap.Uint64("ts", readTxn.StartTS()))
	if err != nil {
		panic(err)
	}
	for i := 0; i < maxn; i += 10000 {
		key := []byte(fmt.Sprintf("%s%d", prefix, i))
		val, err := readTxn.Get(context.Background(), key)
		if err != nil {
			logutil.BgLogger().Error("get failed", zap.String("key", string(key)), zap.Error(err))
			panic(err)
		}
		if !bytes.Equal(value, val) {
			panic(fmt.Sprintf("value mismatch: %v != %v", value, val))
		}
	}
	println(time.Now().String(), "after check")
}

func goLargeTxn(txn *transaction.KVTxn, maxn int, prefix string, value []byte) {
	buffer := txn.GetMemBuffer().(*unionstore.TikvBuffer)
	for i := 0; i < maxn; i++ {
		if i%100000 == 0 {
			println(time.Now().String(), i)
		}
		key := []byte(fmt.Sprintf("%s%d", prefix, i))
		err := buffer.Set(key, value)
		if err != nil {
			panic(err)
		}
	}

	println(time.Now().String(), "before commit")
	err := buffer.Commit()
	println(time.Now().String(), "after commit")
	if err != nil {
		panic(err)
	}
}

func goNormalTxn(txn *transaction.KVTxn, maxn int, prefix string, value []byte) {
	for i := 0; i < maxn; i++ {
		if i%100000 == 0 {
			println(time.Now().String(), i)
		}
		key := []byte(fmt.Sprintf("%s%d", prefix, i))
		err := txn.Set(key, value)
		if err != nil {
			panic(err)
		}
	}

	txn.SetEnableAsyncCommit(false)
	txn.SetEnable1PC(false)
	println(time.Now().String(), "before commit")
	err := txn.Commit(context.Background())
	println(time.Now().String(), "after commit")
	if err != nil {
		panic(err)
	}
}
