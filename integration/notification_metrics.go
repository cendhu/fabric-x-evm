/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-x-evm/gateway/core"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/notification"
)

type notificationReceiverMetrics struct {
	mu sync.Mutex

	recvBatches        uint64
	recvTxs            uint64
	recvWaitTotal      time.Duration
	recvWaitMax        time.Duration
	convertTotal       time.Duration
	convertMax         time.Duration
	handlerStatsByName map[string]*notificationHandlerStats
}

type notificationHandlerStats struct {
	count uint64
	txs   uint64
	total time.Duration
	max   time.Duration
}

type notificationReceiverMetricsSnapshot struct {
	RecvBatches        uint64
	RecvTxs            uint64
	RecvWaitTotal      time.Duration
	RecvWaitMax        time.Duration
	ConvertTotal       time.Duration
	ConvertMax         time.Duration
	HandlerStatsByName map[string]notificationHandlerStats
}

func notificationReceiverMetricsEnabled() bool {
	return strings.EqualFold(os.Getenv("PERF_NOTIFICATION_METRICS"), "true")
}

func newNotificationReceiverMetrics() *notificationReceiverMetrics {
	return &notificationReceiverMetrics{handlerStatsByName: map[string]*notificationHandlerStats{}}
}

func (m *notificationReceiverMetrics) RecordRecv(txCount int, recvWait, convert time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recvBatches++
	m.recvTxs += uint64(txCount)
	m.recvWaitTotal += recvWait
	if recvWait > m.recvWaitMax {
		m.recvWaitMax = recvWait
	}
	m.convertTotal += convert
	if convert > m.convertMax {
		m.convertMax = convert
	}
}

func (m *notificationReceiverMetrics) recordHandler(name string, txCount int, d time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.handlerStatsByName[name]
	if stats == nil {
		stats = &notificationHandlerStats{}
		m.handlerStatsByName[name] = stats
	}
	stats.count++
	stats.txs += uint64(txCount)
	stats.total += d
	if d > stats.max {
		stats.max = d
	}
}

func (m *notificationReceiverMetrics) WrapTxHandler(name string, handler core.TxHandler) core.TxHandler {
	if m == nil {
		return handler
	}
	return measuredTxHandler{name: name, handler: handler, metrics: m}
}

func (m *notificationReceiverMetrics) WrapAllTxHandler(name string, handler notification.AllTxHandler) notification.AllTxHandler {
	if m == nil {
		return handler
	}
	return measuredAllTxHandler{name: name, handler: handler, metrics: m}
}

func (m *notificationReceiverMetrics) SnapshotAndReset() notificationReceiverMetricsSnapshot {
	if m == nil {
		return notificationReceiverMetricsSnapshot{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := notificationReceiverMetricsSnapshot{
		RecvBatches:        m.recvBatches,
		RecvTxs:            m.recvTxs,
		RecvWaitTotal:      m.recvWaitTotal,
		RecvWaitMax:        m.recvWaitMax,
		ConvertTotal:       m.convertTotal,
		ConvertMax:         m.convertMax,
		HandlerStatsByName: map[string]notificationHandlerStats{},
	}
	for name, stats := range m.handlerStatsByName {
		out.HandlerStatsByName[name] = *stats
	}
	m.recvBatches = 0
	m.recvTxs = 0
	m.recvWaitTotal = 0
	m.recvWaitMax = 0
	m.convertTotal = 0
	m.convertMax = 0
	m.handlerStatsByName = map[string]*notificationHandlerStats{}
	return out
}

func (m *notificationReceiverMetrics) LogPeriodically(ctx context.Context, interval time.Duration, log sdk.Logger) {
	if m == nil || interval <= 0 || log == nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Infof(m.SnapshotAndReset().Format())
		}
	}
}

func (s notificationReceiverMetricsSnapshot) Format() string {
	recvWaitAvg := time.Duration(0)
	convertAvg := time.Duration(0)
	if s.RecvBatches > 0 {
		recvWaitAvg = s.RecvWaitTotal / time.Duration(s.RecvBatches)
		convertAvg = s.ConvertTotal / time.Duration(s.RecvBatches)
	}
	parts := []string{fmt.Sprintf("notif_recv batches=%d txs=%d recv_wait_avg=%.3fms recv_wait_max=%.3fms convert_avg=%.3fms convert_max=%.3fms",
		s.RecvBatches, s.RecvTxs,
		durationMillis(recvWaitAvg), durationMillis(s.RecvWaitMax),
		durationMillis(convertAvg), durationMillis(s.ConvertMax))}

	names := make([]string, 0, len(s.HandlerStatsByName))
	for name := range s.HandlerStatsByName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		stats := s.HandlerStatsByName[name]
		avg := time.Duration(0)
		if stats.count > 0 {
			avg = stats.total / time.Duration(stats.count)
		}
		parts = append(parts, fmt.Sprintf("handler[%s] count=%d txs=%d avg=%.3fms max=%.3fms",
			name, stats.count, stats.txs, durationMillis(avg), durationMillis(stats.max)))
	}
	return strings.Join(parts, " | ")
}

type measuredTxHandler struct {
	name    string
	handler core.TxHandler
	metrics *notificationReceiverMetrics
}

func (h measuredTxHandler) HandleTx(ctx context.Context, notifs []core.TxNotification) error {
	start := time.Now()
	err := h.handler.HandleTx(ctx, notifs)
	h.metrics.recordHandler(h.name, len(notifs), time.Since(start))
	return err
}

type measuredAllTxHandler struct {
	name    string
	handler notification.AllTxHandler
	metrics *notificationReceiverMetrics
}

func (h measuredAllTxHandler) HandleBatch(ctx context.Context, batch notification.AllTxBatch) error {
	start := time.Now()
	err := h.handler.HandleBatch(ctx, batch)
	h.metrics.recordHandler(h.name, len(batch.Events), time.Since(start))
	return err
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
