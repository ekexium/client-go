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

package transaction

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/internal/locate"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

type actionPipelinedFlush struct{}

var _ twoPhaseCommitAction = actionPipelinedFlush{}

func (action actionPipelinedFlush) String() string {
	return "pipelined_flush"
}

func (action actionPipelinedFlush) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return nil
}

func (c *twoPhaseCommitter) buildPipelinedFlushRequest(batch batchMutations) *tikvrpc.Request {
	m := batch.mutations
	mutations := make([]*kvrpcpb.Mutation, m.Len())

	for i := 0; i < m.Len(); i++ {
		assertion := kvrpcpb.Assertion_None
		if m.IsAssertExists(i) {
			assertion = kvrpcpb.Assertion_Exist
		}
		if m.IsAssertNotExist(i) {
			assertion = kvrpcpb.Assertion_NotExist
		}
		mutations[i] = &kvrpcpb.Mutation{
			Op:        m.GetOp(i),
			Key:       m.GetKey(i),
			Value:     m.GetValue(i),
			Assertion: assertion,
		}
	}

	minCommitTS := c.startTS + 1

	req := &kvrpcpb.FlushRequest{
		Mutations:   mutations,
		PrimaryKey:  c.primary(),
		StartTs:     c.startTS,
		MinCommitTs: minCommitTS,
	}

	r := tikvrpc.NewRequest(
		tikvrpc.CmdFlush, req, kvrpcpb.Context{
			Priority:               c.priority,
			SyncLog:                c.syncLog,
			ResourceGroupTag:       c.resourceGroupTag,
			DiskFullOpt:            c.diskFullOpt,
			TxnSource:              c.txnSource,
			MaxExecutionDurationMs: uint64(client.MaxWriteExecutionTime.Milliseconds()),
			RequestSource:          c.txn.GetRequestSource(),
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: c.resourceGroupName,
			},
		},
	)
	return r
}

func (action actionPipelinedFlush) handleSingleBatch(
	c *twoPhaseCommitter, bo *retry.Backoffer, batch batchMutations,
) (err error) {
	if len(c.primaryKey) == 0 {
		return errors.New("primary key should be set before pipelined flush")
	}

	tBegin := time.Now()
	attempts := 0

	req := c.buildPipelinedFlushRequest(batch)
	sender := locate.NewRegionRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient())

	for {
		attempts++
		reqBegin := time.Now()
		if reqBegin.Sub(tBegin) > slowRequestThreshold {
			logutil.BgLogger().Warn(
				"slow pipelined flush request",
				zap.Uint64("startTS", c.startTS),
				zap.Stringer("region", &batch.region),
				zap.Int("attempts", attempts),
			)
			tBegin = time.Now()
		}
		resp, _, err := sender.SendReq(bo, req, batch.region, client.ReadTimeoutShort)
		// Unexpected error occurs, return it
		if err != nil {
			return err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return err
		}
		if regionErr != nil {
			// For other region error and the fake region error, backoff because
			// there's something wrong.
			// For the real EpochNotMatch error, don't backoff.
			if regionErr.GetEpochNotMatch() == nil || locate.IsFakeRegionError(regionErr) {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					return err
				}
			}
			if regionErr.GetDiskFull() != nil {
				storeIds := regionErr.GetDiskFull().GetStoreId()
				desc := " "
				for _, i := range storeIds {
					desc += strconv.FormatUint(i, 10) + " "
				}

				logutil.Logger(bo.GetCtx()).Error(
					"Request failed cause of TiKV disk full",
					zap.String("store_id", desc),
					zap.String("reason", regionErr.GetDiskFull().GetReason()),
				)

				return errors.New(regionErr.String())
			}
			same, err := batch.relocate(bo, c.store.GetRegionCache())
			if err != nil {
				return err
			}
			if same {
				continue
			}
			err = c.doActionOnMutations(bo, actionPipelinedFlush{}, batch.mutations)
			return err
		}
		if resp.Resp == nil {
			return errors.WithStack(tikverr.ErrBodyMissing)
		}
		flushResp := resp.Resp.(*kvrpcpb.FlushResponse)
		keyErrs := flushResp.GetErrors()
		if len(keyErrs) == 0 {
			// Clear the RPC Error since the request is evaluated successfully.
			sender.SetRPCError(nil)

			// Update CommitDetails
			reqDuration := time.Since(reqBegin)
			c.getDetail().MergeFlushReqDetails(
				reqDuration,
				batch.region.GetID(),
				sender.GetStoreAddr(),
				flushResp.ExecDetailsV2,
			)

			if batch.isPrimary {
				// start keepalive after primary key is written.
				c.run(c, nil)
			}
			return nil
		}
	}
}

func (c *twoPhaseCommitter) pipelinedFlushMutations(bo *retry.Backoffer, mutations CommitterMutations) error {
	if span := opentracing.SpanFromContext(bo.GetCtx()); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("twoPhaseCommitter.pipelinedFlushMutations", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		bo.SetCtx(opentracing.ContextWithSpan(bo.GetCtx(), span1))
	}

	return c.doActionOnMutations(bo, actionPipelinedFlush{}, mutations)
}

func (c *twoPhaseCommitter) commitFlushedMutations(bo *retry.Backoffer) error {
	commitTS, err := c.store.GetTimestampWithRetry(bo, c.txn.GetScope())
	if err != nil {
		logutil.Logger(bo.GetCtx()).Warn("commit pipelined transaction get commitTS failed",
			zap.Error(err),
			zap.Uint64("txnStartTS", c.startTS))
		return err
	}
	atomic.StoreUint64(&c.commitTS, commitTS)

	primaryMutation := NewPlainMutations(1)
	primaryMutation.Push(c.primaryOp, c.primaryKey, nil, false, false, false, false)
	if err = c.commitMutations(bo, &primaryMutation); err != nil {
		return errors.Trace(err)
	}
	// async resolve the rest locks.
	commitBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
	go c.resolveFlushedLocks(commitBo, c.pipelinedStart, c.pipelinedEnd)
	return nil
}

func (c *twoPhaseCommitter) resolveFlushedLocks(bo *retry.Backoffer, start, end []byte) {
	regionCache := c.store.GetRegionCache()
	resolvedRegions := 0
	for {
		loc, err := regionCache.LocateKey(bo, start)
		if err != nil {
			err = bo.Backoff(retry.BoRegionMiss, err)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("locate key error", zap.Error(err))
				return
			}
			continue
		}

		lreq := &kvrpcpb.ResolveLockRequest{
			StartVersion:  c.startTS,
			CommitVersion: atomic.LoadUint64(&c.commitTS),
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdResolveLock, lreq, kvrpcpb.Context{
			ResourceControlContext: &kvrpcpb.ResourceControlContext{
				ResourceGroupName: c.txn.GetRequestSource(),
			},
		})
		req.RequestSource = c.txn.GetRequestSource()
		resp, err := c.store.SendReq(bo, req, loc.Region, client.ReadTimeoutShort)
		if err != nil {
			err = bo.Backoff(retry.BoRegionMiss, err)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("send resolve lock request error", zap.Error(err))
				return
			}
			continue
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			logutil.Logger(bo.GetCtx()).Error("get region error failed", zap.Error(err))
			return
		}
		if regionErr != nil {
			err = bo.Backoff(retry.BoRegionMiss, err)
			if err != nil {
				logutil.Logger(bo.GetCtx()).Error("send resolve lock get region error", zap.Error(err))
				return
			}
			continue
		}
		if resp.Resp == nil {
			logutil.Logger(bo.GetCtx()).Error("send resolve lock response body missing", zap.Error(errors.WithStack(tikverr.ErrBodyMissing)))
			return
		}
		cmdResp := resp.Resp.(*kvrpcpb.ResolveLockResponse)
		if keyErr := cmdResp.GetError(); keyErr != nil {
			err = errors.Errorf("unexpected resolve err: %s", keyErr)
			logutil.BgLogger().Error("resolveLock error", zap.Error(err))
			return
		}
		resolvedRegions++
		if bytes.Compare(loc.EndKey, end) > 0 {
			logutil.BgLogger().Info("Commit pipelined transaction done",
				zap.Int("resolved regions", resolvedRegions))
			return
		}
		start = loc.EndKey
	}
}
