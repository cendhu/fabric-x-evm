/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-evm/gateway/core"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"github.com/stretchr/testify/require"
)

type testTxHandler struct{}

func (testTxHandler) HandleTx(context.Context, []core.TxNotification) error { return nil }

type testAllTxHandler struct{}

func (testAllTxHandler) HandleBatch(context.Context, notification.AllTxBatch) error { return nil }

func TestNotificationReceiverMetricsRecordReceiverAndHandlerDurations(t *testing.T) {
	metrics := newNotificationReceiverMetrics()
	metrics.RecordRecv(10, time.Millisecond, 2*time.Millisecond)

	wrappedTx := metrics.WrapTxHandler("txqueue", testTxHandler{})
	require.NoError(t, wrappedTx.HandleTx(context.Background(), make([]core.TxNotification, 10)))

	wrappedBatch := metrics.WrapAllTxHandler("dispatcher", testAllTxHandler{})
	require.NoError(t, wrappedBatch.HandleBatch(context.Background(), notification.AllTxBatch{Events: make([]notification.CommittedTxEvent, 10)}))

	line := metrics.SnapshotAndReset().Format()
	require.True(t, strings.Contains(line, "notif_recv batches=1 txs=10"), line)
	require.True(t, strings.Contains(line, "recv_wait_avg=1.000ms"), line)
	require.True(t, strings.Contains(line, "convert_avg=2.000ms"), line)
	require.True(t, strings.Contains(line, "handler[txqueue] count=1 txs=10"), line)
	require.True(t, strings.Contains(line, "handler[dispatcher] count=1 txs=10"), line)
}
