/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/notification"
	"google.golang.org/grpc"
)

type notificationConnectionPeer interface {
	Connection() *grpc.ClientConn
}

type measuredAllTxPeer struct {
	peer    notificationConnectionPeer
	metrics *notificationReceiverMetrics
}

func (p measuredAllTxPeer) StreamAllTransactions(ctx context.Context, req *notification.StreamAllRequest, processor notification.AllTxProcessor) error {
	stream, err := committerpb.NewNotifierClient(p.peer.Connection()).StreamAllTransactions(ctx, toMeasuredProtoStreamAllRequest(req))
	if err != nil {
		return fmt.Errorf("open stream-all-transactions: %w", err)
	}

	for {
		recvStart := time.Now()
		batch, err := stream.Recv()
		recvWait := time.Since(recvStart)
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("recv: %w", err)
		}

		convertStart := time.Now()
		sdkBatch := convertMeasuredTxEventBatch(batch)
		convertDuration := time.Since(convertStart)
		p.metrics.RecordRecv(len(sdkBatch.Events), recvWait, convertDuration)

		if err := processor.ProcessBatch(ctx, sdkBatch); err != nil {
			return fmt.Errorf("process batch: %w", err)
		}
	}
}

func toMeasuredProtoStreamAllRequest(req *notification.StreamAllRequest) *committerpb.StreamAllRequest {
	if req == nil {
		return &committerpb.StreamAllRequest{}
	}
	return &committerpb.StreamAllRequest{
		FilterNamespaces:     req.FilterNamespaces,
		FilterStatus:         req.FilterStatus,
		IncludeReadWriteSets: req.IncludeReadWriteSets,
		IncludeEndorsements:  req.IncludeEndorsements,
	}
}

func convertMeasuredTxEventBatch(batch *committerpb.TxEventBatch) notification.AllTxBatch {
	if batch == nil {
		return notification.AllTxBatch{}
	}
	events := make([]notification.CommittedTxEvent, len(batch.GetEvents()))
	for i, e := range batch.GetEvents() {
		ref := e.GetRef()
		blockNum := batch.GetBlockNumber()
		var txID string
		var txNum uint32
		if ref != nil {
			txID = ref.GetTxId()
			if ref.GetBlockNum() != 0 {
				blockNum = ref.GetBlockNum()
			}
			txNum = ref.GetTxNum()
		}
		events[i] = notification.CommittedTxEvent{
			TxID:         txID,
			BlockNum:     blockNum,
			TxNum:        txNum,
			Status:       e.GetStatus(),
			Namespaces:   cloneNamespaces(e.GetNamespaces()),
			Endorsements: append([]*applicationpb.Endorsements(nil), e.GetEndorsements()...),
		}
	}
	return notification.AllTxBatch{BlockNumber: batch.GetBlockNumber(), Events: events}
}

func cloneNamespaces(namespaces []*applicationpb.TxNamespace) []*applicationpb.TxNamespace {
	if len(namespaces) == 0 {
		return nil
	}
	out := make([]*applicationpb.TxNamespace, len(namespaces))
	copy(out, namespaces)
	return out
}
