/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package mockfabricx

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Metrics struct {
	broadcastRecv           atomic.Uint64
	broadcastAck            atomic.Uint64
	broadcastSubmitNanos    atomic.Uint64
	broadcastSubmitMaxNanos atomic.Uint64

	blocksCommitted   atomic.Uint64
	blockTxsCommitted atomic.Uint64
	commitNanos       atomic.Uint64
	commitMaxNanos    atomic.Uint64

	blockSubDrops atomic.Uint64
	eventSubDrops atomic.Uint64

	deliverBlocksSent   atomic.Uint64
	deliverSendNanos    atomic.Uint64
	deliverSendMaxNanos atomic.Uint64

	notifyBatchesSent  atomic.Uint64
	notifyTxsSent      atomic.Uint64
	notifySendNanos    atomic.Uint64
	notifySendMaxNanos atomic.Uint64

	eventSubQueueDepth    atomic.Int64
	eventSubQueueCapacity atomic.Int64
}

type MetricsSnapshot struct {
	BroadcastRecv           uint64
	BroadcastAck            uint64
	BroadcastSubmitNanos    uint64
	BroadcastSubmitMaxNanos uint64
	BlocksCommitted         uint64
	BlockTxsCommitted       uint64
	CommitNanos             uint64
	CommitMaxNanos          uint64
	BlockSubDrops           uint64
	EventSubDrops           uint64
	DeliverBlocksSent       uint64
	DeliverSendNanos        uint64
	DeliverSendMaxNanos     uint64
	NotifyBatchesSent       uint64
	NotifyTxsSent           uint64
	NotifySendNanos         uint64
	NotifySendMaxNanos      uint64
	OrdererQueueDepth       int
	OrdererQueueCapacity    int
	EventSubQueueDepth      int
	EventSubQueueCapacity   int
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) RecordBroadcastRecv() {
	if m != nil {
		m.broadcastRecv.Add(1)
	}
}

func (m *Metrics) RecordBroadcastAck() {
	if m != nil {
		m.broadcastAck.Add(1)
	}
}

func (m *Metrics) RecordBroadcastSubmit(d time.Duration) {
	if m == nil {
		return
	}
	nanos := uint64(d.Nanoseconds())
	m.broadcastSubmitNanos.Add(nanos)
	atomicMax(&m.broadcastSubmitMaxNanos, nanos)
}

func (m *Metrics) RecordCommit(txCount int, d time.Duration) {
	if m == nil {
		return
	}
	m.blocksCommitted.Add(1)
	m.blockTxsCommitted.Add(uint64(txCount))
	nanos := uint64(d.Nanoseconds())
	m.commitNanos.Add(nanos)
	atomicMax(&m.commitMaxNanos, nanos)
}

func (m *Metrics) RecordBlockSubDrop() {
	if m != nil {
		m.blockSubDrops.Add(1)
	}
}

func (m *Metrics) RecordEventSubDrop() {
	if m != nil {
		m.eventSubDrops.Add(1)
	}
}

func (m *Metrics) RecordDeliverSend(d time.Duration) {
	if m == nil {
		return
	}
	m.deliverBlocksSent.Add(1)
	nanos := uint64(d.Nanoseconds())
	m.deliverSendNanos.Add(nanos)
	atomicMax(&m.deliverSendMaxNanos, nanos)
}

func (m *Metrics) RecordNotifySend(txCount int, d time.Duration) {
	if m == nil {
		return
	}
	m.notifyBatchesSent.Add(1)
	m.notifyTxsSent.Add(uint64(txCount))
	nanos := uint64(d.Nanoseconds())
	m.notifySendNanos.Add(nanos)
	atomicMax(&m.notifySendMaxNanos, nanos)
}

func (m *Metrics) RecordEventSubQueueDepth(depth, capacity int) {
	if m == nil {
		return
	}
	m.eventSubQueueDepth.Store(int64(depth))
	m.eventSubQueueCapacity.Store(int64(capacity))
}

func (m *Metrics) Snapshot(queueDepth, queueCapacity int) MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{OrdererQueueDepth: queueDepth, OrdererQueueCapacity: queueCapacity}
	}
	return MetricsSnapshot{
		BroadcastRecv:           m.broadcastRecv.Load(),
		BroadcastAck:            m.broadcastAck.Load(),
		BroadcastSubmitNanos:    m.broadcastSubmitNanos.Load(),
		BroadcastSubmitMaxNanos: m.broadcastSubmitMaxNanos.Load(),
		BlocksCommitted:         m.blocksCommitted.Load(),
		BlockTxsCommitted:       m.blockTxsCommitted.Load(),
		CommitNanos:             m.commitNanos.Load(),
		CommitMaxNanos:          m.commitMaxNanos.Load(),
		BlockSubDrops:           m.blockSubDrops.Load(),
		EventSubDrops:           m.eventSubDrops.Load(),
		DeliverBlocksSent:       m.deliverBlocksSent.Load(),
		DeliverSendNanos:        m.deliverSendNanos.Load(),
		DeliverSendMaxNanos:     m.deliverSendMaxNanos.Load(),
		NotifyBatchesSent:       m.notifyBatchesSent.Load(),
		NotifyTxsSent:           m.notifyTxsSent.Load(),
		NotifySendNanos:         m.notifySendNanos.Load(),
		NotifySendMaxNanos:      m.notifySendMaxNanos.Load(),
		OrdererQueueDepth:       queueDepth,
		OrdererQueueCapacity:    queueCapacity,
		EventSubQueueDepth:      int(m.eventSubQueueDepth.Load()),
		EventSubQueueCapacity:   int(m.eventSubQueueCapacity.Load()),
	}
}

func (m *Metrics) LogPeriodically(ctx context.Context, interval time.Duration, w io.Writer, queueDepth func() (int, int)) {
	if m == nil || interval <= 0 || w == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	prevTime := time.Now()
	prev := m.Snapshot(0, 0)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			depth, capacity := 0, 0
			if queueDepth != nil {
				depth, capacity = queueDepth()
			}
			current := m.Snapshot(depth, capacity)
			elapsed := now.Sub(prevTime).Seconds()
			fmt.Fprintln(w, current.FormatDelta(prev, elapsed))
			prev = current
			prevTime = now
		}
	}
}

func (s MetricsSnapshot) FormatDelta(prev MetricsSnapshot, elapsedSeconds float64) string {
	if elapsedSeconds <= 0 {
		elapsedSeconds = 1
	}
	broadcastRecvDelta := s.BroadcastRecv - prev.BroadcastRecv
	broadcastAckDelta := s.BroadcastAck - prev.BroadcastAck
	blockDelta := s.BlocksCommitted - prev.BlocksCommitted
	blockTxDelta := s.BlockTxsCommitted - prev.BlockTxsCommitted
	notifyBatchDelta := s.NotifyBatchesSent - prev.NotifyBatchesSent
	notifyTxDelta := s.NotifyTxsSent - prev.NotifyTxsSent
	deliverBlockDelta := s.DeliverBlocksSent - prev.DeliverBlocksSent

	return fmt.Sprintf(
		"mockfabricx metrics broadcast_rx=%.2f/s broadcast_ack=%.2f/s orderer_queue=%d/%d event_sub_queue=%d/%d blocks=%.2f/s block_txs=%.2f/s block_avg=%.1f commit_avg=%.3fms commit_max=%.3fms notify_batches=%.2f/s notify_txs=%.2f/s notify_send_avg=%.3fms notify_send_max=%.3fms deliver_blocks=%.2f/s deliver_send_avg=%.3fms deliver_send_max=%.3fms drops(block=%d event=%d)",
		rate(broadcastRecvDelta, elapsedSeconds),
		rate(broadcastAckDelta, elapsedSeconds),
		s.OrdererQueueDepth,
		s.OrdererQueueCapacity,
		s.EventSubQueueDepth,
		s.EventSubQueueCapacity,
		rate(blockDelta, elapsedSeconds),
		rate(blockTxDelta, elapsedSeconds),
		avgCount(blockTxDelta, blockDelta),
		durationMillis(avgNanos(s.CommitNanos-prev.CommitNanos, blockDelta)),
		durationMillis(time.Duration(s.CommitMaxNanos)),
		rate(notifyBatchDelta, elapsedSeconds),
		rate(notifyTxDelta, elapsedSeconds),
		durationMillis(avgNanos(s.NotifySendNanos-prev.NotifySendNanos, notifyBatchDelta)),
		durationMillis(time.Duration(s.NotifySendMaxNanos)),
		rate(deliverBlockDelta, elapsedSeconds),
		durationMillis(avgNanos(s.DeliverSendNanos-prev.DeliverSendNanos, deliverBlockDelta)),
		durationMillis(time.Duration(s.DeliverSendMaxNanos)),
		s.BlockSubDrops,
		s.EventSubDrops,
	)
}

func rate(delta uint64, elapsedSeconds float64) float64 {
	return float64(delta) / elapsedSeconds
}

func avgCount(total, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

func avgNanos(total uint64, count uint64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(total / count)
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func atomicMax(v *atomic.Uint64, candidate uint64) {
	for {
		current := v.Load()
		if candidate <= current || v.CompareAndSwap(current, candidate) {
			return
		}
	}
}
