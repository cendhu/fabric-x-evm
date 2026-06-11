/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package mockfabricx

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricsSnapshotRecordsMockRates(t *testing.T) {
	metrics := NewMetrics()
	metrics.RecordBroadcastRecv()
	metrics.RecordBroadcastAck()
	metrics.RecordBroadcastSubmit(2 * time.Millisecond)
	metrics.RecordCommit(1000, 3*time.Millisecond)
	metrics.RecordNotifySend(1000, 4*time.Millisecond)
	metrics.RecordDeliverSend(5 * time.Millisecond)
	metrics.RecordBlockSubDrop()
	metrics.RecordEventSubDrop()

	metrics.RecordEventSubQueueDepth(9, 1024)
	snapshot := metrics.Snapshot(7, 64)

	require.Equal(t, uint64(1), snapshot.BroadcastRecv)
	require.Equal(t, uint64(1), snapshot.BroadcastAck)
	require.Equal(t, uint64(1), snapshot.BlocksCommitted)
	require.Equal(t, uint64(1000), snapshot.BlockTxsCommitted)
	require.Equal(t, uint64(1), snapshot.NotifyBatchesSent)
	require.Equal(t, uint64(1000), snapshot.NotifyTxsSent)
	require.Equal(t, uint64(1), snapshot.DeliverBlocksSent)
	require.Equal(t, uint64(1), snapshot.BlockSubDrops)
	require.Equal(t, uint64(1), snapshot.EventSubDrops)
	require.Equal(t, 7, snapshot.OrdererQueueDepth)
	require.Equal(t, 64, snapshot.OrdererQueueCapacity)
	require.Equal(t, 9, snapshot.EventSubQueueDepth)
	require.Equal(t, 1024, snapshot.EventSubQueueCapacity)
}

func TestMetricsSnapshotFormatsDelta(t *testing.T) {
	prev := MetricsSnapshot{}
	current := MetricsSnapshot{
		BroadcastRecv:         10,
		BroadcastAck:          10,
		BlocksCommitted:       2,
		BlockTxsCommitted:     1000,
		NotifyBatchesSent:     2,
		NotifyTxsSent:         1000,
		DeliverBlocksSent:     2,
		OrdererQueueDepth:     3,
		OrdererQueueCapacity:  64,
		EventSubQueueDepth:    9,
		EventSubQueueCapacity: 1024,
	}

	line := current.FormatDelta(prev, 2)

	require.True(t, strings.Contains(line, "broadcast_rx=5.00/s"), line)
	require.True(t, strings.Contains(line, "block_txs=500.00/s"), line)
	require.True(t, strings.Contains(line, "notify_txs=500.00/s"), line)
	require.True(t, strings.Contains(line, "orderer_queue=3/64"), line)
	require.True(t, strings.Contains(line, "event_sub_queue=9/1024"), line)
}
