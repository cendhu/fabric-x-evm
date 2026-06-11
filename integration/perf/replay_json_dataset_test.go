/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-evm/endorser"
	econf "github.com/hyperledger/fabric-x-evm/endorser/config"
	"github.com/hyperledger/fabric-x-evm/endorser/testimpl"
	gwcore "github.com/hyperledger/fabric-x-evm/gateway/core"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	gwtestimpl "github.com/hyperledger/fabric-x-evm/gateway/testimpl"
	"github.com/hyperledger/fabric-x-evm/integration"
	"github.com/hyperledger/fabric-x-evm/integration/contracts"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/grpclog"
)

// TxCompletionTracker forwards all transaction completion notifications to a single channel.
// It implements gwcore.TxHandler to receive notifications from the notification system.
type TxCompletionTracker struct {
	completionCh chan gwcore.TxNotification
}

// NewTxCompletionTracker creates a new tracker with a completion channel.
func NewTxCompletionTracker(completionCh chan gwcore.TxNotification) *TxCompletionTracker {
	return &TxCompletionTracker{
		completionCh: completionCh,
	}
}

// HandleTx implements gwcore.TxHandler. It receives notifications about completed transactions
// and forwards them to the completion channel.
func (t *TxCompletionTracker) HandleTx(ctx context.Context, notifs []gwcore.TxNotification) error {
	for _, notif := range notifs {
		select {
		case t.completionCh <- notif:
		default:
			// Channel full - this shouldn't happen with proper sizing
			return fmt.Errorf("completion channel full, dropping notification for tx %s", notif.EthTxHash.Hex())
		}
	}
	return nil
}

// balancePrimingEndorserFactory creates endorsers with balance priming support for testing.
func balancePrimingEndorserFactory(balancePriming *testimpl.BalancePrimingConfig) integration.EndorserFactory {
	return func(t *testing.T, ecfg econf.Endorser, channel, namespace string, evmConfig endorser.EVMConfig, protocol string) (endorser.KVS, endorsement.Builder, gwcore.Endorser) {
		// Create the base endorser components
		db, builder, baseEndorser := integration.NewEndorser(t, ecfg, channel, namespace, evmConfig, protocol)

		// Extract the base EVMEngine
		baseEngine, ok := baseEndorser.Engine.(*endorser.EVMEngine)
		if !ok {
			t.Fatalf("Expected *endorser.EVMEngine, got %T", baseEndorser.Engine)
		}

		// Wrap the engine with balance priming support
		wrappedEngine := testimpl.NewEVMEngineWrapper(
			namespace,
			db,
			evmConfig,
			protocol == "fabric-x", // monotonicVersions
			baseEngine,
		)
		wrappedEngine.SetBalancePriming(balancePriming)

		// Replace the engine in the endorser
		baseEndorser.Engine = wrappedEngine

		return db, builder, baseEndorser
	}
}

const defaultReplayDatasetPath = "testdata/synthetic_usdc_400k.tsv.gz"

type replayConfig struct {
	// windowSize is the number of transfers to use from the dataset.
	// 0 means use the entire dataset.
	windowSize int

	// wrapAround, when true, restarts the feed from the beginning of the
	// window after every pass. The feed continues until totalDispatches
	// transfers have been sent to workChan. Ignored when false.
	wrapAround bool

	// wrapCount is the raw wrap count requested by configuration.
	// totalDispatches is computed later, after the effective window size is known.
	wrapCount int64

	// totalDispatches is the total number of transfers to dispatch when
	// wrapAround is true. Ignored when wrapAround is false.
	totalDispatches int64
}

type replayTxQueueMetricsSnapshot struct {
	DequeueCount       int64
	DequeueWaitTotal   time.Duration
	DequeueWaitMax     time.Duration
	HandleTxCount      int64
	HandleTxBatchTotal int64
	HandleTxTotal      time.Duration
	HandleTxMax        time.Duration
}

type replayTxQueueMetrics struct {
	queue gwcore.TxQueueInterface

	mu                 sync.Mutex
	dequeueCount       int64
	dequeueWaitTotal   time.Duration
	dequeueWaitMax     time.Duration
	handleTxCount      int64
	handleTxBatchTotal int64
	handleTxTotal      time.Duration
	handleTxMax        time.Duration
}

func newReplayTxQueueMetrics(queue gwcore.TxQueueInterface) *replayTxQueueMetrics {
	return &replayTxQueueMetrics{queue: queue}
}

func (m *replayTxQueueMetrics) Enqueue(tx *types.Transaction) {
	m.queue.Enqueue(tx)
}

func (m *replayTxQueueMetrics) Dequeue() (*types.Transaction, bool) {
	start := time.Now()
	tx, ok := m.queue.Dequeue()
	wait := time.Since(start)

	m.mu.Lock()
	m.dequeueCount++
	m.dequeueWaitTotal += wait
	if wait > m.dequeueWaitMax {
		m.dequeueWaitMax = wait
	}
	m.mu.Unlock()

	return tx, ok
}

func (m *replayTxQueueMetrics) IsPending(txHash common.Hash) *types.Transaction {
	return m.queue.IsPending(txHash)
}

func (m *replayTxQueueMetrics) Close() {
	m.queue.Close()
}

func (m *replayTxQueueMetrics) Handle(ctx context.Context, block *domain.Block) error {
	return m.queue.Handle(ctx, block)
}

func (m *replayTxQueueMetrics) Stats() (int, int, int, int) {
	return m.queue.Stats()
}

func (m *replayTxQueueMetrics) HandleTx(ctx context.Context, notifs []gwcore.TxNotification) error {
	handler, ok := m.queue.(gwcore.TxHandler)
	if !ok {
		return fmt.Errorf("wrapped queue %T does not implement TxHandler", m.queue)
	}

	start := time.Now()
	err := handler.HandleTx(ctx, notifs)
	elapsed := time.Since(start)

	m.mu.Lock()
	m.handleTxCount++
	m.handleTxBatchTotal += int64(len(notifs))
	m.handleTxTotal += elapsed
	if elapsed > m.handleTxMax {
		m.handleTxMax = elapsed
	}
	m.mu.Unlock()

	return err
}

func (m *replayTxQueueMetrics) SnapshotAndReset() replayTxQueueMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := replayTxQueueMetricsSnapshot{
		DequeueCount:       m.dequeueCount,
		DequeueWaitTotal:   m.dequeueWaitTotal,
		DequeueWaitMax:     m.dequeueWaitMax,
		HandleTxCount:      m.handleTxCount,
		HandleTxBatchTotal: m.handleTxBatchTotal,
		HandleTxTotal:      m.handleTxTotal,
		HandleTxMax:        m.handleTxMax,
	}
	m.dequeueCount = 0
	m.dequeueWaitTotal = 0
	m.dequeueWaitMax = 0
	m.handleTxCount = 0
	m.handleTxBatchTotal = 0
	m.handleTxTotal = 0
	m.handleTxMax = 0
	return snapshot
}

type replayGatewayProcessMetricsSnapshot struct {
	ProcessStarted  int64
	ProcessFinished int64
	ProcessErrors   int64
	ProcessTotal    time.Duration
	ProcessMax      time.Duration
	EndorseCount    int64
	EndorseErrors   int64
	EndorseTotal    time.Duration
	EndorseMax      time.Duration
	SubmitCount     int64
	SubmitErrors    int64
	SubmitTotal     time.Duration
	SubmitMax       time.Duration
	ActiveProcess   int64
	ActiveEndorse   int64
	ActiveSubmit    int64
}

type replayGatewayProcessMetrics struct {
	mu sync.Mutex

	processStarted  int64
	processFinished int64
	processErrors   int64
	processTotal    time.Duration
	processMax      time.Duration
	endorseCount    int64
	endorseErrors   int64
	endorseTotal    time.Duration
	endorseMax      time.Duration
	submitCount     int64
	submitErrors    int64
	submitTotal     time.Duration
	submitMax       time.Duration
	activeProcess   int64
	activeEndorse   int64
	activeSubmit    int64
}

func (m *replayGatewayProcessMetrics) ProcessStarted() {
	m.mu.Lock()
	m.processStarted++
	m.activeProcess++
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) EndorsementStarted() {
	m.mu.Lock()
	m.activeEndorse++
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) EndorsementFinished(duration time.Duration, err error) {
	m.mu.Lock()
	m.endorseCount++
	m.endorseTotal += duration
	if duration > m.endorseMax {
		m.endorseMax = duration
	}
	if err != nil {
		m.endorseErrors++
	}
	if m.activeEndorse > 0 {
		m.activeEndorse--
	}
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) SubmitStarted() {
	m.mu.Lock()
	m.activeSubmit++
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) SubmitFinished(duration time.Duration, err error) {
	m.mu.Lock()
	m.submitCount++
	m.submitTotal += duration
	if duration > m.submitMax {
		m.submitMax = duration
	}
	if err != nil {
		m.submitErrors++
	}
	if m.activeSubmit > 0 {
		m.activeSubmit--
	}
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) ProcessFinished(duration time.Duration, err error) {
	m.mu.Lock()
	m.processFinished++
	m.processTotal += duration
	if duration > m.processMax {
		m.processMax = duration
	}
	if err != nil {
		m.processErrors++
	}
	if m.activeProcess > 0 {
		m.activeProcess--
	}
	m.mu.Unlock()
}

func (m *replayGatewayProcessMetrics) SnapshotAndReset() replayGatewayProcessMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := replayGatewayProcessMetricsSnapshot{
		ProcessStarted:  m.processStarted,
		ProcessFinished: m.processFinished,
		ProcessErrors:   m.processErrors,
		ProcessTotal:    m.processTotal,
		ProcessMax:      m.processMax,
		EndorseCount:    m.endorseCount,
		EndorseErrors:   m.endorseErrors,
		EndorseTotal:    m.endorseTotal,
		EndorseMax:      m.endorseMax,
		SubmitCount:     m.submitCount,
		SubmitErrors:    m.submitErrors,
		SubmitTotal:     m.submitTotal,
		SubmitMax:       m.submitMax,
		ActiveProcess:   m.activeProcess,
		ActiveEndorse:   m.activeEndorse,
		ActiveSubmit:    m.activeSubmit,
	}
	m.processStarted = 0
	m.processFinished = 0
	m.processErrors = 0
	m.processTotal = 0
	m.processMax = 0
	m.endorseCount = 0
	m.endorseErrors = 0
	m.endorseTotal = 0
	m.endorseMax = 0
	m.submitCount = 0
	m.submitErrors = 0
	m.submitTotal = 0
	m.submitMax = 0
	return snapshot
}

type replayBatchSubmitMetricsSnapshot struct {
	Count  int64
	Errors int64
	Total  time.Duration
	Max    time.Duration
}

type replayBatchSubmitMetrics struct {
	mu     sync.Mutex
	count  int64
	errors int64
	total  time.Duration
	max    time.Duration
}

func (m *replayBatchSubmitMetrics) BatchSubmitFinished(duration time.Duration, err error) {
	m.mu.Lock()
	m.count++
	m.total += duration
	if duration > m.max {
		m.max = duration
	}
	if err != nil {
		m.errors++
	}
	m.mu.Unlock()
}

func (m *replayBatchSubmitMetrics) SnapshotAndReset() replayBatchSubmitMetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := replayBatchSubmitMetricsSnapshot{
		Count:  m.count,
		Errors: m.errors,
		Total:  m.total,
		Max:    m.max,
	}
	m.count = 0
	m.errors = 0
	m.total = 0
	m.max = 0
	return snapshot
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func loadReplayConfigFromEnv(t *testing.T) replayConfig {
	cfg := replayConfig{windowSize: 3000, wrapAround: false}

	windowSizeStr := os.Getenv("PERF_REPLAY_WINDOW_SIZE")
	if windowSizeStr != "" {
		var parsedWindowSize int
		_, err := fmt.Sscanf(windowSizeStr, "%d", &parsedWindowSize)
		assert.NoError(t, err, "PERF_REPLAY_WINDOW_SIZE must be a valid integer")
		assert.True(t, parsedWindowSize >= 0, "PERF_REPLAY_WINDOW_SIZE must be >= 0")
		cfg.windowSize = parsedWindowSize
	}

	if cfg.windowSize == 0 {
		t.Log("WARNING: full dataset mode selected — this is intended for distributed infra, not local runs")
	}

	wrapCountStr := os.Getenv("PERF_REPLAY_WRAP_COUNT")
	if wrapCountStr != "" {
		var wrapCount int64
		_, err := fmt.Sscanf(wrapCountStr, "%d", &wrapCount)
		assert.NoError(t, err, "PERF_REPLAY_WRAP_COUNT must be a valid integer")
		assert.True(t, wrapCount >= 1, "PERF_REPLAY_WRAP_COUNT must be >= 1")
		cfg.wrapCount = wrapCount
		if wrapCount > 1 {
			cfg.wrapAround = true
		}
	}

	return cfg
}

func loadReplayWindowFromPath(t *testing.T, datasetPath string, cfg replayConfig) []TokenTransfer {
	t.Helper()

	transfers, needsTransactions := loadReplayDatasetFromPath(t, datasetPath)
	window := transfers
	if cfg.windowSize > 0 && cfg.windowSize < len(transfers) {
		window = transfers[:cfg.windowSize]
	}
	if needsTransactions {
		assert.NoError(t, populateReplayTransactions(t.Context(), window))
	}
	return window
}

func loadReplayDatasetFromPath(t *testing.T, datasetPath string) ([]TokenTransfer, bool) {
	t.Helper()

	if strings.HasSuffix(datasetPath, ".tsv.gz") {
		transfers, err := ParseTSVGZ(datasetPath)
		assert.NoError(t, err)
		assert.NotEmpty(t, transfers, "dataset should contain transfers")
		return transfers, true
	}

	file, err := os.Open(datasetPath)
	assert.NoError(t, err)
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	assert.NoError(t, err)
	defer gzReader.Close()

	var transfers []TokenTransfer
	decoder := json.NewDecoder(gzReader)
	err = decoder.Decode(&transfers)
	assert.NoError(t, err)
	assert.NotEmpty(t, transfers, "dataset should contain transfers")
	return transfers, false
}

func populateReplayTransactions(ctx context.Context, transfers []TokenTransfer) error {
	nonceTracker := NewNonceTracker()
	chainConfig := *params.AllEthashProtocolChanges
	chainConfig.ChainID = big.NewInt(4011)

	for i := range transfers {
		transfer := &transfers[i]
		ethSender, err := integration.NewEthClientFromAddress(transfer.Sender, contracts.FiatTokenV22MetaData, &chainConfig)
		if err != nil {
			return fmt.Errorf("transfer %d: create eth client for sender %s: %w", i, transfer.Sender.Hex(), err)
		}

		mappedRecipient := mapAddress(transfer.Recipient)
		tx, err := ethSender.TxForCall(ctx, nonceTracker, &usdcAddress, "transfer", mappedRecipient, transfer.Value.ToBig())
		if err != nil {
			return fmt.Errorf("transfer %d: create transaction: %w", i, err)
		}

		transfer.Transaction, err = tx.MarshalBinary()
		if err != nil {
			return fmt.Errorf("transfer %d: marshal transaction: %w", i, err)
		}
	}

	return nil
}

//lint:ignore U1000 kept for future tests / debugging
func logMem(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("[%s] Alloc = %d MB | TotalAlloc = %d MB | Sys = %d MB | NumGC = %d\n",
		tag,
		m.Alloc/1024/1024,
		m.TotalAlloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC,
	)
}

//lint:ignore U1000 kept for future tests / debugging
func writeHeapProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	runtime.GC() // normalize heap before snapshot
	if err := pprof.WriteHeapProfile(f); err != nil {
		panic(err)
	}
}

// runReplayTest executes the replay test with configurable worker counts and returns metrics.
// Returns: (overallThroughput, failedTransactionCount, totalTransactionCount)
func runReplayTest(t *testing.T, processingWorkerCount int, submittingWorkerCount int, numOutstandingTx int, cfg replayConfig) (float64, int64, int64) {
	// Silence GRPC logging
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, os.Stderr, os.Stderr))

	// USDC contract address
	USDCAddr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	// Configure balance priming for USDC transfers
	balancePriming := &testimpl.BalancePrimingConfig{
		Enabled:         true,
		ContractAddress: USDCAddr,
		MappingPosition: 9, // USDC balance mapping is at slot 9
	}
	evmConfig := endorser.EVMConfig{}

	// Setup test harness with USDC contract and balance priming enabled
	factory := balancePrimingEndorserFactory(balancePriming)

	// Create completion channel for transaction notifications
	completionCh := make(chan gwcore.TxNotification, numOutstandingTx*2)

	// Create completion tracker for async transaction monitoring
	tracker := NewTxCompletionTracker(completionCh)

	// Choose test harness based on backend:
	// - Local: Traditional block-based synchronization
	// - Fabric: Traditional block-based synchronization
	// - Fabric-X: Notification-based (MemoryStore + NotificationDispatcher)
	txQueueMetrics := newReplayTxQueueMetrics(gwcore.NewTxQueueV2())
	gatewayProcessMetrics := &replayGatewayProcessMetrics{}
	batchSubmitMetrics := &replayBatchSubmitMetrics{}
	// th, err := integration.NewLocalTestHarnessWithFactoryAndTxQueue(t, integration.TestLogger{T: t}, evmConfig, "testdata/USDC_contract.json", "fabric", map[string]any{"Gateway.WorkerCount": processingWorkerCount}, factory, txQueueMetrics)
	th, err := integration.NewFabricXTestHarnessWithNotifications(t, integration.TestLogger{T: t}, evmConfig, "testdata/USDC_contract.json", map[string]any{"Gateway.WorkerCount": processingWorkerCount}, factory, txQueueMetrics, tracker)
	// th, err = integration.NewFabricTestHarnessWithFactoryAndTxQueue(t, integration.TestLogger{T: t}, evmConfig, "testdata/USDC_contract.json", map[string]any{"Gateway.WorkerCount": processingWorkerCount}, factory, txQueueMetrics)
	assert.NoError(t, err)
	th.Gateways[0].SetProcessTimingObserver(gatewayProcessMetrics)
	th.BatchSubmitters[0].SetSubmitTimingObserver(batchSubmitMetrics)

	// Wrap the gateway with NonceBypassGateway to skip nonce validation
	// This is necessary for wrap-around replay where the same transactions are replayed
	wrappedGateway := gwtestimpl.NewNonceBypassGateway(th.Gateways[0])

	// Load the replay dataset.
	t.Logf("Loading dataset from %s", defaultReplayDatasetPath)
	window := loadReplayWindowFromPath(t, defaultReplayDatasetPath, cfg)

	t.Logf("Loaded %d transfers from dataset window", len(window))

	if cfg.wrapAround && cfg.wrapCount > 0 {
		cfg.totalDispatches = int64(len(window)) * cfg.wrapCount
	}

	// Validate numOutstandingTx
	if numOutstandingTx > len(window) {
		panic(fmt.Sprintf("numOutstandingTx (%d) cannot be larger than window size (%d)", numOutstandingTx, len(window)))
	}

	// Replay transactions with parallel workers
	// Atomic counters for thread-safe counting
	var successCount, failCount, skippedCount int64

	runtime.GC()

	// Track throughput
	startTime := time.Now()
	var lastLogTime atomic.Value
	lastLogTime.Store(startTime)
	var lastLogCount int64

	// Create a channel for work items
	type workItem struct {
		index    int64
		transfer TokenTransfer
	}
	// Buffer size = numOutstandingTx + numWorkers to avoid blocking
	workChan := make(chan workItem, numOutstandingTx+submittingWorkerCount)

	// Metrics for outstanding transactions
	var outstandingTxCount int64

	// Worker pool configuration
	numWorkers := submittingWorkerCount
	var wg sync.WaitGroup

	// Start worker goroutines - they continuously submit without waiting for completion
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for item := range workChan {
				i := item.index
				transfer := item.transfer

				// Unmarshal the transaction from bytes
				tx := new(types.Transaction)
				err := tx.UnmarshalBinary(transfer.Transaction)
				if err != nil {
					t.Logf("Transfer %d: Failed to unmarshal transaction: %v", i, err)
					panic(err)
				}

				// Send the transaction without waiting for completion
				// Use the wrapped gateway directly to bypass nonce validation
				err = wrappedGateway.SendTransaction(context.Background(), tx)
				if err != nil {
					t.Logf("Transfer %d: SendTransaction error: %v", i, err)
					atomic.AddInt64(&failCount, 1)
					atomic.AddInt64(&outstandingTxCount, -1)
					continue
				}
				// Transaction submitted successfully - it's now outstanding
				// The completion will be tracked by the refill goroutine
			}
		}()
	}

	// Progress logging goroutine
	stopLogging := make(chan struct{})
	var loggingWg sync.WaitGroup
	loggingWg.Add(1)
	go func() {
		defer loggingWg.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		itrctr := 0

		for {
			select {
			case <-ticker.C:
				now := time.Now()
				lastTime := lastLogTime.Load().(time.Time)
				elapsed := now.Sub(lastTime).Seconds()

				currentSuccess := atomic.LoadInt64(&successCount)
				currentFail := atomic.LoadInt64(&failCount)
				currentSkipped := atomic.LoadInt64(&skippedCount)
				currentTotal := currentSuccess + currentFail
				currentOutstanding := atomic.LoadInt64(&outstandingTxCount)

				txProcessed := currentTotal - lastLogCount
				throughput := float64(txProcessed) / elapsed

				totalElapsed := now.Sub(startTime).Seconds()
				overallThroughput := float64(currentTotal) / totalElapsed

				progressTarget := int64(len(window))
				if cfg.wrapAround {
					progressTarget = cfg.totalDispatches
				}

				queueMetrics := txQueueMetrics.SnapshotAndReset()
				processMetrics := gatewayProcessMetrics.SnapshotAndReset()
				batchMetrics := batchSubmitMetrics.SnapshotAndReset()

				dequeueWaitAvg := time.Duration(0)
				if queueMetrics.DequeueCount > 0 {
					dequeueWaitAvg = queueMetrics.DequeueWaitTotal / time.Duration(queueMetrics.DequeueCount)
				}
				handleTxAvg := time.Duration(0)
				handleTxBatchAvg := float64(0)
				if queueMetrics.HandleTxCount > 0 {
					handleTxAvg = queueMetrics.HandleTxTotal / time.Duration(queueMetrics.HandleTxCount)
					handleTxBatchAvg = float64(queueMetrics.HandleTxBatchTotal) / float64(queueMetrics.HandleTxCount)
				}
				processAvg := time.Duration(0)
				if processMetrics.ProcessFinished > 0 {
					processAvg = processMetrics.ProcessTotal / time.Duration(processMetrics.ProcessFinished)
				}
				endorseAvg := time.Duration(0)
				if processMetrics.EndorseCount > 0 {
					endorseAvg = processMetrics.EndorseTotal / time.Duration(processMetrics.EndorseCount)
				}
				submitEnqueueAvg := time.Duration(0)
				if processMetrics.SubmitCount > 0 {
					submitEnqueueAvg = processMetrics.SubmitTotal / time.Duration(processMetrics.SubmitCount)
				}
				batchSubmitAvg := time.Duration(0)
				if batchMetrics.Count > 0 {
					batchSubmitAvg = batchMetrics.Total / time.Duration(batchMetrics.Count)
				}

				t.Logf("Progress: %d/%d transfers processed (%d successful, %d failed, %d skipped, %d outstanding) | Throughput: %.2f tx/s (recent), %.2f tx/s (overall) | gateway process n=%d done=%d err=%d active(p/e/s)=%d/%d/%d avg/max=%.3f/%.3fms endorse n=%d err=%d avg/max=%.3f/%.3fms enqueue n=%d err=%d avg/max=%.3f/%.3fms batch_submit n=%d err=%d avg/max=%.3f/%.3fms queue_dequeue n=%d avg/max=%.3f/%.3fms handle_tx batches=%d avg_batch=%.1f avg/max=%.3f/%.3fms",
					currentSuccess+currentFail+currentSkipped, progressTarget,
					currentSuccess, currentFail, currentSkipped, currentOutstanding,
					throughput, overallThroughput,
					processMetrics.ProcessStarted, processMetrics.ProcessFinished, processMetrics.ProcessErrors,
					processMetrics.ActiveProcess, processMetrics.ActiveEndorse, processMetrics.ActiveSubmit,
					durationMillis(processAvg), durationMillis(processMetrics.ProcessMax),
					processMetrics.EndorseCount, processMetrics.EndorseErrors, durationMillis(endorseAvg), durationMillis(processMetrics.EndorseMax),
					processMetrics.SubmitCount, processMetrics.SubmitErrors, durationMillis(submitEnqueueAvg), durationMillis(processMetrics.SubmitMax),
					batchMetrics.Count, batchMetrics.Errors, durationMillis(batchSubmitAvg), durationMillis(batchMetrics.Max),
					queueMetrics.DequeueCount, durationMillis(dequeueWaitAvg), durationMillis(queueMetrics.DequeueWaitMax),
					queueMetrics.HandleTxCount, handleTxBatchAvg, durationMillis(handleTxAvg), durationMillis(queueMetrics.HandleTxMax))

				// Poll committer block height
				if th.BlockHeightPeer != nil {
					if bh, err := th.BlockHeightPeer.BlockHeight(context.Background()); err == nil {
						t.Logf("Committer block height: %d", bh)
					}
				}

				// Update for next interval
				lastLogTime.Store(now)
				lastLogCount = currentTotal

				_ = itrctr
				// runtime.GC()
				// logMem("blah")
				// itrctr++
				// writeHeapProfile(fmt.Sprintf("heap_%d.prof", itrctr))

			case <-stopLogging:
				return
			}
		}
	}()

	// Feed work to the workers (refill goroutine)
	var refillWg sync.WaitGroup
	refillWg.Add(1)
	var dispatched int64
	cursor := 0

	go func() {
		defer refillWg.Done()
		defer close(workChan)

		// Pre-fill the channel with numOutstandingTx transactions
		t.Logf("Pre-filling work channel with %d transactions", numOutstandingTx)
		for range numOutstandingTx {
			workChan <- workItem{index: dispatched, transfer: window[cursor]}
			atomic.AddInt64(&outstandingTxCount, 1)
			dispatched++
			cursor++

		}
		t.Logf("Pre-fill complete, %d transactions dispatched", dispatched)

		// Process completions and refill
		for notif := range completionCh {
			atomic.AddInt64(&outstandingTxCount, -1)

			// Update success/fail counts
			if notif.Status == committerpb.Status_COMMITTED {
				atomic.AddInt64(&successCount, 1)
			} else {
				atomic.AddInt64(&failCount, 1)
				t.Logf("Transaction %s failed with status: %v", notif.EthTxHash.Hex(), notif.Status)
			}

			// Check if we should dispatch more work
			if cfg.wrapAround {
				if dispatched >= cfg.totalDispatches {
					// Check if all outstanding transactions are done
					if atomic.LoadInt64(&outstandingTxCount) == 0 {
						t.Logf("All transactions completed, closing work channel")
						return
					}
					continue
				}
			} else {
				if cursor >= len(window) {
					// Check if all outstanding transactions are done
					if atomic.LoadInt64(&outstandingTxCount) == 0 {
						t.Logf("All transactions completed, closing work channel")
						return
					}
					continue
				}
			}

			// Add next transaction to the channel
			workChan <- workItem{index: dispatched, transfer: window[cursor]}
			atomic.AddInt64(&outstandingTxCount, 1)
			dispatched++
			cursor++

			// Handle wrap-around
			if cursor >= len(window) {
				if cfg.wrapAround {
					cursor = 0
					// BalancePrimingWrapper.GetNonce() handles nonce validation bypass automatically,
					// so no explicit nonce priming is needed between wrap-around passes.
					t.Logf("Wrap-around: restarting from beginning (dispatched %d so far)", dispatched)
				}
			}
		}
	}()

	// Wait for all workers to finish processing
	wg.Wait()

	// Close completion channel to signal refill goroutine
	close(completionCh)

	// Wait for refill goroutine to finish
	refillWg.Wait()

	// Stop the logging goroutine
	close(stopLogging)
	loggingWg.Wait()

	// Final counts
	finalSuccess := atomic.LoadInt64(&successCount)
	finalFail := atomic.LoadInt64(&failCount)
	finalSkipped := atomic.LoadInt64(&skippedCount)

	t.Logf("Replay complete: %d successful, %d failed, %d skipped out of %d total transfers",
		finalSuccess, finalFail, finalSkipped, dispatched)

	// Calculate overall throughput
	totalElapsed := time.Since(startTime).Seconds()
	overallThroughput := float64(finalSuccess+finalFail) / totalElapsed

	// Return metrics (throughput, failed count, total dispatched transfers)
	return overallThroughput, finalFail, dispatched
}

// TestReplayJSONDataset loads the USDC_dataset.json.gz file with pre-generated transactions
// and replays them with batched priming of sender balances.
func TestReplayJSONDataset(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	// flogging.ActivateSpec("gateway.core.txqueue_v2=debug")

	// Run the test with 64 processors, 1 submitter, and 2000 outstanding transactions.
	_, _, _ = runReplayTest(t, 64, 1, 2000, loadReplayConfigFromEnv(t))
}

type performanceResult struct {
	processingWorkers  int
	submittingWorkers  int
	throughput         float64
	failedTransactions int64
	totalTransactions  int64
	failureRate        float64
}

// TestReplayJSONDatasetPerformance runs the replay test with varying worker counts
// to measure performance characteristics across different configurations.
func TestReplayJSONDatasetPerformance(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	// Define the range of worker counts to test
	processingWorkerCounts := []int{64}
	submittingWorkerCounts := []int{1}

	// Store results
	var results []performanceResult

	t.Logf("Starting performance test with varying worker counts...")

	// Run tests with different worker configurations
	for _, processingWorkers := range processingWorkerCounts {
		for _, submittingWorkers := range submittingWorkerCounts {
			t.Logf("\n=== Testing with processingWorkers=%d, submittingWorkers=%d ===",
				processingWorkers, submittingWorkers)

			throughput, failedTxs, totalTxs := runReplayTest(t, processingWorkers, submittingWorkers, 1000, loadReplayConfigFromEnv(t))
			failureRate := float64(failedTxs) / float64(totalTxs)

			results = append(results, performanceResult{
				processingWorkers:  processingWorkers,
				submittingWorkers:  submittingWorkers,
				throughput:         throughput,
				failedTransactions: failedTxs,
				totalTransactions:  totalTxs,
				failureRate:        failureRate,
			})

			t.Logf("Result: Throughput=%.2f tx/s, Failed=%d/%d (%.2f%%)",
				throughput, failedTxs, totalTxs, failureRate*100)
		}
	}

	// Write results to CSV file
	csvPath := "performance_results.csv"
	file, err := os.Create(csvPath)
	assert.NoError(t, err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	err = writer.Write([]string{
		"processing_workers",
		"submitting_workers",
		"throughput_tx_per_s",
		"failed_transactions",
		"total_transactions",
		"failure_rate",
	})
	assert.NoError(t, err)

	// Write data rows
	for _, result := range results {
		err = writer.Write([]string{
			fmt.Sprintf("%d", result.processingWorkers),
			fmt.Sprintf("%d", result.submittingWorkers),
			fmt.Sprintf("%.2f", result.throughput),
			fmt.Sprintf("%d", result.failedTransactions),
			fmt.Sprintf("%d", result.totalTransactions),
			fmt.Sprintf("%.4f", result.failureRate),
		})
		assert.NoError(t, err)
	}

	t.Logf("Performance results written to %s", csvPath)
}
