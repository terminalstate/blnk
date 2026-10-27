/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real lifecycle and handler tests for the worker process, against the local
// Postgres, Redis, and Typesense (the suite's hard dependencies).
//
// Not covered on purpose: the max-retry branch of processTransaction needs
// asynq.GetRetryCount, which only carries a value inside a running asynq
// server context; its decision helper hasReachedMaxRetryAttempt is unit-
// tested separately in workers_test.go.

func TestProcessTransaction_AppliesQueuedTransaction(t *testing.T) {
	b := newCmdTestInstance(t)
	src, dst := createBalancePair(t, b)

	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 50, true)
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task))

	// The money must actually have moved.
	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "5000", dstAfter.Balance.String(), "destination must be credited 50 at precision 100")
}

func TestProcessTransaction_MalformedPayload(t *testing.T) {
	b := newCmdTestInstance(t)

	task := asynq.NewTask("transaction_queue_cmd_test_1", []byte("{not json"))
	err := b.processTransaction(context.Background(), task)
	require.Error(t, err, "a malformed payload must error so asynq can retry/dead-letter it")
}

func TestProcessTransaction_InsufficientFundsRejects(t *testing.T) {
	b := newCmdTestInstance(t)
	// Disable retries so the insufficient-funds branch rejects immediately.
	b.cnf.Queue.InsufficientFundRetries = false

	src, dst := createBalancePair(t, b)

	// No overdraft and an empty source balance: processing must fail with
	// insufficient funds and the transaction must be REJECTED, not retried.
	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 75, false)
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task), "rejection is a handled outcome, not a handler error")

	recorded, err := b.blnk.GetTransactionByRef(context.Background(), txn.Reference)
	require.NoError(t, err)
	assert.Equal(t, "REJECTED", recorded.Status)

	// The destination must not have been credited.
	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "0", dstAfter.Balance.String())
}

func TestProcessTransaction_DuplicateReferenceAcks(t *testing.T) {
	b := newCmdTestInstance(t)
	src, dst := createBalancePair(t, b)

	txn := queuedTransaction(src.BalanceID, dst.BalanceID, 50, true)
	original := *txn
	payload, err := json.Marshal(txn)
	require.NoError(t, err)

	task := asynq.NewTask("transaction_queue_cmd_test_1", payload)
	require.NoError(t, b.processTransaction(context.Background(), task))

	replay := original
	replay.TransactionID = model.GenerateUUIDWithSuffix("txn")
	replayPayload, err := json.Marshal(&replay)
	require.NoError(t, err)

	replayTask := asynq.NewTask("transaction_queue_cmd_test_1", replayPayload)
	require.NoError(t, b.processTransaction(context.Background(), replayTask), "duplicate reference is a handled outcome, not a handler error")

	dstAfter, err := b.blnk.GetDataSource().GetBalanceByIDLite(dst.BalanceID)
	require.NoError(t, err)
	assert.Equal(t, "5000", dstAfter.Balance.String(), "destination must not be credited twice")
}

// An atomic bulk request in the default queued mode, run by the real worker,
// which takes the items in whatever order asynq hands them out. When one item
// is rejected, nothing from the batch may stay applied, and
// bulk_transaction.failed must report the rollback it actually made.
func TestWorker_AtomicBulkRejectionRollsBackQueuedBatch(t *testing.T) {
	cfg := realInfraConfig(t)
	suffix := strings.ToLower(gofakeit.LetterN(8))
	cfg.Queue.TransactionQueue = "transaction_queue_cmd_test_bulk_" + suffix
	cfg.Queue.WebhookQueue = "webhook_queue_cmd_test_bulk_" + suffix
	cfg.Queue.InsufficientFundRetries = false
	// Webhooks are only enqueued here; nothing serves this queue.
	cfg.Notification.Webhook.Url = "http://127.0.0.1:1/bulk"
	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")
	b := &blnkInstance{blnk: newBlnk, cnf: cfg}
	ctx := context.Background()

	srv, err := initializeWorkerServer(cfg, initializeQueues())
	require.NoError(t, err)
	mux := asynq.NewServeMux()
	initializeTaskHandlers(b, mux)
	require.NoError(t, srv.Start(mux))

	queue := cfg.Queue.TransactionQueue + "_1"
	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: cfg.Redis.Dns})
	db, err := sql.Open("postgres", cfg.DataSource.Dns)
	require.NoError(t, err)
	t.Cleanup(func() {
		srv.Shutdown()
		_ = inspector.DeleteQueue(queue, true)
		_ = inspector.DeleteQueue(cfg.Queue.WebhookQueue, true)
		_ = inspector.Close()
		_ = db.Close()
	})

	// drain waits until the worker has nothing left to run, refunds included.
	drain := func() {
		deadline := time.Now().Add(30 * time.Second)
		for idle := time.Now(); time.Since(idle) < time.Second; time.Sleep(50 * time.Millisecond) {
			require.True(t, time.Now().Before(deadline), "the transaction queue did not drain")
			info, err := inspector.GetQueueInfo(queue)
			if err != nil || info.Pending+info.Active+info.Scheduled+info.Retry > 0 {
				idle = time.Now()
			}
		}
	}
	failedWebhooks := func(batchID string) []string {
		tasks, err := inspector.ListPendingTasks(cfg.Queue.WebhookQueue, asynq.PageSize(1000))
		require.NoError(t, err)
		var messages []string
		for _, task := range tasks {
			var hook struct {
				Event string `json:"event"`
				Data  struct {
					BatchID string `json:"batch_id"`
					Error   string `json:"error"`
				} `json:"data"`
			}
			require.NoError(t, json.Unmarshal(task.Payload, &hook))
			if hook.Event == "bulk_transaction.failed" && hook.Data.BatchID == batchID {
				messages = append(messages, hook.Data.Error)
			}
		}
		return messages
	}
	newBalance := func() *model.Balance {
		balance, err := b.blnk.GetDataSource().CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
		require.NoError(t, err)
		return &balance
	}
	balanceOf := func(id string) *model.Balance {
		balance, err := b.blnk.GetDataSource().GetBalanceByIDLite(id)
		require.NoError(t, err)
		return balance
	}

	// The source of each batch holds 100: 500 cannot be paid, 50 and 10 can.
	// Every case is sent five times and all batches go to the worker at once,
	// so the items are applied in varying orders.
	type batch struct {
		name     string
		inflight bool
		amounts  []float64
		id       string
		src      *model.Balance
		dsts     []*model.Balance
	}
	var batches []*batch
	for _, inflight := range []bool{false, true} {
		for _, amounts := range [][]float64{{50, 500}, {50, 500, 10}} {
			for run := 0; run < 5; run++ {
				bt := &batch{name: fmt.Sprintf("inflight=%v %v run %d", inflight, amounts, run), inflight: inflight, amounts: amounts}
				funding := newBalance()
				bt.src = newBalance()
				funded := queuedTransaction(funding.BalanceID, bt.src.BalanceID, 100, true)
				funded.SkipQueue = true
				_, err := b.blnk.QueueTransaction(ctx, funded)
				require.NoError(t, err)

				var items []*model.Transaction
				for _, amount := range amounts {
					dst := newBalance()
					bt.dsts = append(bt.dsts, dst)
					items = append(items, &model.Transaction{Reference: "ref_" + gofakeit.UUID(), Source: bt.src.BalanceID, Destination: dst.BalanceID, Amount: amount, Precision: 100, Currency: "USD"})
				}
				result, err := b.blnk.CreateBulkTransactions(ctx, &model.BulkTransactionRequest{Atomic: true, Inflight: inflight, Transactions: items})
				require.NoError(t, err)
				bt.id = result.BatchID
				batches = append(batches, bt)
			}
		}
	}
	drain()

	for _, bt := range batches {
		srcAfter := balanceOf(bt.src.BalanceID)
		assert.Equal(t, "10000", srcAfter.Balance.String(), "%s: the source must be back at 100", bt.name)
		assert.Equal(t, "0", srcAfter.InflightDebitBalance.String(), "%s: no inflight debit may stay", bt.name)
		for i, dst := range bt.dsts {
			dstAfter := balanceOf(dst.BalanceID)
			assert.Equal(t, "0", dstAfter.Balance.String(), "%s: item %d must not stay applied", bt.name, i)
			assert.Equal(t, "0", dstAfter.InflightCreditBalance.String(), "%s: item %d must not stay inflight", bt.name, i)
		}

		// Items the worker had applied (or put inflight) before the
		// rejection; the webhook must report exactly these as rolled back.
		status, kind, action := "APPLIED", "applied", "refunded"
		if bt.inflight {
			status, kind, action = "INFLIGHT", "inflight", "voided"
		}
		var held int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM blnk.transactions
			WHERE status = $2 AND meta_data->>'QUEUED_PARENT_TRANSACTION' = $1
			AND reference NOT LIKE '%_refund'`, bt.id, status).Scan(&held))
		messages := failedWebhooks(bt.id)
		require.Len(t, messages, 1, "%s: one bulk_transaction.failed per batch", bt.name)
		if held == 0 {
			assert.Contains(t, messages[0], "Nothing from this batch", "%s", bt.name)
		} else {
			assert.Contains(t, messages[0], fmt.Sprintf("%d %s transaction(s) from this batch have been %s", held, kind, action), "%s", bt.name)
		}

		// Every other item ends REJECTED: the one that failed, and the
		// ones the worker reached after it, which were never applied.
		var rejected int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM blnk.transactions
			WHERE status = 'REJECTED' AND meta_data->>'QUEUED_PARENT_TRANSACTION' = $1`, bt.id).Scan(&rejected))
		assert.Equal(t, len(bt.amounts)-held, rejected, "%s: the items not applied must be rejected", bt.name)
	}
}

func TestProcessInflightCommitAndExpiry(t *testing.T) {
	b := newCmdTestInstance(t)
	ctx := context.Background()

	startInflight := func() string {
		src, dst := createBalancePair(t, b)
		txn := queuedTransaction(src.BalanceID, dst.BalanceID, 20, true)
		txn.Inflight = true
		recorded, err := b.blnk.RecordTransaction(ctx, txn)
		require.NoError(t, err)
		require.Equal(t, "INFLIGHT", recorded.Status)
		return recorded.TransactionID
	}

	t.Run("commit", func(t *testing.T) {
		txnID := startInflight()
		payload, err := json.Marshal(txnID)
		require.NoError(t, err)

		task := asynq.NewTask("inflight_commit_cmd_test", payload)
		require.NoError(t, b.processInflightCommit(ctx, task))

		committed, err := b.blnk.GetTransaction(ctx, txnID)
		require.NoError(t, err)
		// Committing creates a child COMMIT entry; the parent leaves INFLIGHT.
		assert.NotEqual(t, "VOID", committed.Status)

		// Committing again is idempotent: the already-committed leg is terminal,
		// so the worker acks (nil) instead of double-committing.
		err = b.processInflightCommit(ctx, task)
		require.NoError(t, err, "a replayed commit must be acked, not errored")
	})

	t.Run("expiry voids", func(t *testing.T) {
		txnID := startInflight()
		payload, err := json.Marshal(txnID)
		require.NoError(t, err)

		task := asynq.NewTask("inflight_expiry_cmd_test", payload)
		require.NoError(t, b.processInflightExpiry(ctx, task))

		// Voiding again must fail: it has already been voided.
		err = b.processInflightExpiry(ctx, task)
		require.Error(t, err, "double void must surface an error")
	})

	t.Run("malformed payload", func(t *testing.T) {
		task := asynq.NewTask("inflight_commit_cmd_test", []byte("{bad"))
		require.Error(t, b.processInflightCommit(ctx, task))
		require.Error(t, b.processInflightExpiry(ctx, task))
	})
}

func TestIndexData(t *testing.T) {
	b := newCmdTestInstance(t)
	ctx := context.Background()

	t.Run("disabled without typesense DNS", func(t *testing.T) {
		task := asynq.NewTask("index_queue_cmd_test", []byte("ignored"))
		require.NoError(t, b.indexData(ctx, task), "indexing must be a silent no-op when search is not configured")
	})

	b.cnf.TypeSenseKey = "blnk-api-key"
	b.cnf.TypeSense = config.TypeSenseConfig{Dns: "http://localhost:8108"}

	t.Run("malformed payload errors", func(t *testing.T) {
		task := asynq.NewTask("index_queue_cmd_test", []byte("{bad json"))
		require.Error(t, b.indexData(ctx, task))
		require.Error(t, b.indexBatchData(ctx, task))
	})

	t.Run("indexes a document into typesense", func(t *testing.T) {
		payload, err := json.Marshal(blnk.IndexTask{
			Collection: "ledgers",
			Payload: map[string]interface{}{
				"ledger_id":  "ldg_worker_idx_test",
				"name":       "worker indexed ledger",
				"created_at": time.Now().Format(time.RFC3339),
			},
		})
		require.NoError(t, err)

		task := asynq.NewTask("index_queue_cmd_test", payload)
		require.NoError(t, b.indexData(ctx, task))
	})
}

func TestRunWorkers_LifecycleStartsAndShutsDown(t *testing.T) {
	b := newCmdTestInstance(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runWorkers(ctx, b, b.cnf) }()

	// The monitoring server proves the worker stack came up.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://localhost:" + b.cnf.Queue.MonitoringPort + "/health")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "worker monitoring server must come up")

	// Cancel = SIGTERM equivalent; the full stack must shut down cleanly.
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(45 * time.Second):
		t.Fatal("runWorkers did not shut down after context cancellation")
	}

	// Monitoring server must be gone after shutdown.
	_, err := http.Get("http://localhost:" + b.cnf.Queue.MonitoringPort + "/health")
	require.Error(t, err, "monitoring server must stop with the workers")
}
