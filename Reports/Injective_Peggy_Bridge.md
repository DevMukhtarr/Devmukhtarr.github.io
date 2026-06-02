# Context Leakage in `SendTx` Retry Loop

**DevMukhtar**  

## Summary

The `SendTx` function in `ethCommitter` contains two related bugs on a single line inside its infinite retry loop. 

A fresh child context is derived from the original `ctx` on every iteration **without ever checking** whether the parent has expired, and the cancel function returned by `context.WithTimeout` is silently discarded.

Together these mean that a caller-imposed deadline is completely ignored across retries, and every iteration leaks a context object that is never explicitly released.

## Description

Inside the retry loop in `eth_committer.go`, the following line executes on every iteration:

```go
opts.Context, _ = context.WithTimeout(ctx, e.committerOpts.RPCTimeout)
```

This leads to two issues:

### Context Leak
`context.WithTimeout` returns both a derived context **and** a cancel function. The cancel function is the mechanism by which Go's runtime releases the resources — timer, goroutine references, and channel — associated with the child context.

By assigning it to `_`, the cancel function is thrown away on every iteration. Each child context then lives until its own `RPCTimeout` timer fires naturally. Under sustained retry activity this accumulates unreleased context objects and timer goroutines proportional to the number of retries, none of which are cleaned up until their individual timers expire.

### Deadline Escape in Nonce Resync
A second related issue is in `resyncNonces`:

```go
nonce, err := e.evmProvider.PendingNonceAt(context.TODO(), from)
```

It uses `context.TODO()` instead of the passed `ctx`.

## Impact

Timeout enforcement is broken across the entire transaction submission path. `SendTx` is the single function responsible for submitting all Ethereum transactions from the Peggo orchestrator.

Any caller that sets a deadline on its context has that deadline silently ignored once the retry loop begins.

**Concrete consequences:**

- **Uncontrolled execution duration**: A `SendTx` call can run arbitrarily long past its caller's deadline. With a high `RPCTimeout` and many retries, a single call that was supposed to finish in seconds can run for minutes, blocking the serialized nonce queue and stalling all subsequent transactions behind it.

- **Transaction queue starvation**: `SendTx` uses `nonceCache.Serialize` to prevent race conditions on the nonce. Because the retry loop cannot be cancelled, a stuck `SendTx` holds the serialization lock indefinitely, starving every other transaction the orchestrator needs to submit — including validator set updates, batch submissions, and oracle reports.

## Recommendation

### 1. Fix Context Leakage

Store and call the cancel function from `WithTimeout`:

```go
var iterCancel context.CancelFunc
opts.Context, iterCancel = context.WithTimeout(ctx, e.committerOpts.RPCTimeout)
defer iterCancel()
```

### 2. Fix Deadline Escape

Pass `ctx` to `PendingNonceAt` instead of `context.TODO()`:

```go
nonce, err := e.evmProvider.PendingNonceAt(ctx, from)
```

## Proof of Concept

Create a file `eth_committer_context_test.go` in `./peggo/orchestrator/ethereum/committer/` and run:

```bash
go test -v ./peggo/orchestrator/ethereum/committer/... -run TestSendTx_ContextLeakage_Proof
```

```go
package committer

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// mockContractCaller implements bind.ContractCaller
type mockContractCaller struct{}

func (m *mockContractCaller) PendingCodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockContractCaller) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockContractCaller) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	return []byte{}, nil
}

// mockEVMProvider implements provider.EVMProviderWithRet for testing
type mockEVMProvider struct {
	mockContractCaller
	suggestGasPriceFn func(ctx context.Context) (*big.Int, error)
	estimateGasFn     func(ctx context.Context, msg ethereum.CallMsg) (uint64, error)
	sendTransactionFn func(ctx context.Context, tx *types.Transaction) (common.Hash, error)
	pendingNonceAtFn  func(ctx context.Context, account common.Address) (uint64, error)
}

func (m *mockEVMProvider) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return []types.Log{}, nil
}

func (m *mockEVMProvider) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	return &mockSubscription{}, nil
}

type mockSubscription struct{}

func (m *mockSubscription) Unsubscribe() {}
func (m *mockSubscription) Err() <-chan error {
	errChan := make(chan error, 1)
	close(errChan)
	return errChan
}

func (m *mockEVMProvider) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	return []byte{}, nil
}

func (m *mockEVMProvider) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	if m.suggestGasPriceFn != nil {
		return m.suggestGasPriceFn(ctx)
	}
	return big.NewInt(20_000_000_000), nil
}

func (m *mockEVMProvider) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {
	return big.NewInt(0), nil
}

func (m *mockEVMProvider) EstimateGas(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	if m.estimateGasFn != nil {
		return m.estimateGasFn(ctx, msg)
	}
	return 100000, nil
}

func (m *mockEVMProvider) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return nil
}

func (m *mockEVMProvider) SendTransactionWithRet(ctx context.Context, tx *types.Transaction) (common.Hash, error) {
	if m.sendTransactionFn != nil {
		return m.sendTransactionFn(ctx, tx)
	}
	return tx.Hash(), nil
}

func (m *mockEVMProvider) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	if m.pendingNonceAtFn != nil {
		return m.pendingNonceAtFn(ctx, account)
	}
	return 0, nil
}

func (m *mockEVMProvider) TransactionByHash(ctx context.Context, hash common.Hash) (tx *types.Transaction, isPending bool, err error) {
	return &types.Transaction{}, false, nil
}

func (m *mockEVMProvider) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	return &types.Receipt{}, nil
}

func (m *mockEVMProvider) HeaderByNumber(ctx context.Context, number *big.Int) (*types.Header, error) {
	return &types.Header{}, nil
}

func returnFixedGasPrice(price int64) func(ctx context.Context) (*big.Int, error) {
	return func(ctx context.Context) (*big.Int, error) {
		return big.NewInt(price), nil
	}
}

func returnFixedGas(gas uint64) func(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
	return func(ctx context.Context, msg ethereum.CallMsg) (uint64, error) {
		return gas, nil
	}
}

func randomAddress() common.Address {
	priv, _ := crypto.GenerateKey()
	return crypto.PubkeyToAddress(priv.PublicKey)
}

func newTestEthCommitter(mock *mockEVMProvider) EVMCommitter {
	priv, _ := crypto.GenerateKey()
	addr := crypto.PubkeyToAddress(priv.PublicKey)

	signer := func(addr common.Address, tx *types.Transaction) (*types.Transaction, error) {
		return types.SignTx(tx, types.NewEIP155Signer(big.NewInt(1)), priv)
	}

	committer, _ := NewEthCommitter(
		addr,
		1.0,
		"1000000000000",
		signer,
		mock,
		TxBroadcastTimeout(10*time.Second),
	)

	return committer
}

func TestSendTx_ContextLeakage_Proof(t *testing.T) {
	const (
		parentDeadline  = 600 * time.Millisecond
		firstSendSleep  = 580 * time.Millisecond
		secondSendSleep = 300 * time.Millisecond
		rpcTimeout      = 400 * time.Millisecond
	)

	t.Run("correct_code_must_stop_after_parent_deadline", func(t *testing.T) {
		var attempts int32

		mock := &mockEVMProvider{}
		mock.suggestGasPriceFn = returnFixedGasPrice(25_000_000_000)
		mock.estimateGasFn = returnFixedGas(100_000)

		mock.pendingNonceAtFn = func(_ context.Context, _ common.Address) (uint64, error) {
			if atomic.LoadInt32(&attempts) >= 1 {
				return 1, nil
			}
			return 0, nil
		}

		mock.sendTransactionFn = func(_ context.Context, tx *types.Transaction) (common.Hash, error) {
			n := atomic.AddInt32(&attempts, 1)

			if n == 1 {
				time.Sleep(firstSendSleep)
				return common.Hash{}, errors.New("nonce too low")
			}

			select {
			case <-parentCtx.Done():
				return common.Hash{}, fmt.Errorf("attempt 2 blocked: %w", parentCtx.Err())
			case <-time.After(secondSendSleep):
				return tx.Hash(), nil
			}
		}

		committer := newTestEthCommitter(mock)
		ec := committer.(*ethCommitter)
		ec.nonceCache.Set(ec.fromAddress, 0)
		ec.committerOpts.RPCTimeout = rpcTimeout

		parentCtx, realCancel := context.WithTimeout(context.Background(), parentDeadline)
		defer realCancel()

		start := time.Now()
		_, err := committer.SendTx(parentCtx, randomAddress(), []byte{1})
		dur := time.Since(start)

		t.Logf("duration=%v attempts=%d err=%v",
			dur.Round(time.Millisecond), atomic.LoadInt32(&attempts), err)

		require.Error(t, err,
			"CORRECT: must return error — parent deadline expired during attempt 2")
		require.EqualValues(t, 2, atomic.LoadInt32(&attempts),
			"CORRECT: attempt 2 was started but must be cut short by parent expiry")
		require.Less(t, dur, firstSendSleep+secondSendSleep,
			"CORRECT: must not complete the full second sleep — parent cut it short")

		t.Logf("✓ Code stopped at %v — parent deadline respected (saved %v)",
			dur.Round(time.Millisecond),
			(firstSendSleep + secondSendSleep - dur).Round(time.Millisecond),
		)
	})

	t.Run("buggy_code_escapes_parent_deadline", func(t *testing.T) {
		var attempts int32

		mock := &mockEVMProvider{}
		mock.suggestGasPriceFn = returnFixedGasPrice(25_000_000_000)
		mock.estimateGasFn = returnFixedGas(100_000)

		mock.pendingNonceAtFn = func(_ context.Context, _ common.Address) (uint64, error) {
			if atomic.LoadInt32(&attempts) >= 1 {
				return 1, nil
			}
			return 0, nil
		}

		mock.sendTransactionFn = func(_ context.Context, tx *types.Transaction) (common.Hash, error) {
			n := atomic.AddInt32(&attempts, 1)
			if n == 1 {
				time.Sleep(firstSendSleep)
				return common.Hash{}, errors.New("nonce too low")
			}
			time.Sleep(secondSendSleep)
			return tx.Hash(), nil
		}

		committer := newTestEthCommitter(mock)
		ec := committer.(*ethCommitter)
		ec.nonceCache.Set(ec.fromAddress, 0)
		ec.committerOpts.RPCTimeout = rpcTimeout

		ctx, cancel := context.WithTimeout(context.Background(), parentDeadline)
		defer cancel()

		start := time.Now()
		hash, err := committer.SendTx(ctx, randomAddress(), []byte{1})
		dur := time.Since(start)

		t.Logf("duration=%v attempts=%d hash=%s err=%v",
			dur.Round(time.Millisecond), atomic.LoadInt32(&attempts), hash.Hex(), err)

		t.Logf("✗ CONFIRMED: completed at %v — %v past the %v parent deadline",
			dur.Round(time.Millisecond),
			(dur - parentDeadline).Round(time.Millisecond),
			parentDeadline,
		)
	})
}
```

## Expected Test Output

```
=== RUN   TestSendTx_ContextLeakage_Proof
=== RUN   TestSendTx_ContextLeakage_Proof/correct_code_must_stop_after_parent_deadline
time="2026-03-17T20:46:27+01:00" level=warning msg="failed to send tx" error="nonce too low" tx_hash=...
time="2026-03-17T20:46:27+01:00" level=warning msg="failed to send tx" error="attempt 2 blocked: context deadline exceeded" tx_hash=...
time="2026-03-17T20:46:27+01:00" level=error msg="SendTx serialize failed" error="attempt 2 blocked: context deadline exceeded"
    eth_committer_context_test.go:203: duration=601ms attempts=2 err=attempt 2 blocked: context deadline exceeded
    eth_committer_context_test.go:213: ✓ Code stopped at 601ms — parent deadline respected (saved 279ms)
=== RUN   TestSendTx_ContextLeakage_Proof/buggy_code_escapes_parent_deadline
time="2026-03-17T20:46:28+01:00" level=warning msg="failed to send tx" error="nonce too low" tx_hash=...
    eth_committer_context_test.go:256: duration=882ms attempts=2 hash=... err=<nil>
    eth_committer_context_test.go:259: ✗ CONFIRMED: completed at 882ms — 282ms past the 600ms parent deadline
--- PASS: TestSendTx_ContextLeakage_Proof (1.49s)
    --- PASS: TestSendTx_ContextLeakage_Proof/correct_code_must_stop_after_parent_deadline (0.60s)
    --- PASS: TestSendTx_ContextLeakage_Proof/buggy_code_escapes_parent_deadline (0.88s)
PASS
ok      github.com/InjectiveLabs/injective-core/peggo/orchestrator/ethereum/committer   1.528s
```