/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-evm/common"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

type timingTestSigner struct{}

func (timingTestSigner) Sign(msg []byte) ([]byte, error) { return msg, nil }
func (timingTestSigner) Serialize() ([]byte, error)      { return []byte("creator"), nil }

type timingTestEndorser struct{}

func (timingTestEndorser) ProcessEVMTransaction(ctx context.Context, inv endorsement.Invocation, ethTx *types.Transaction) (*peer.ProposalResponse, error) {
	return &peer.ProposalResponse{Response: &peer.Response{Status: 200}}, nil
}
func (timingTestEndorser) ProcessCall(ctx context.Context, callMsg *ethereum.CallMsg, blockNumber *big.Int) (*peer.ProposalResponse, error) {
	return nil, nil
}
func (timingTestEndorser) ProcessStateQuery(ctx context.Context, query common.StateQuery) (*peer.ProposalResponse, error) {
	return nil, nil
}

type recordingProcessObserver struct {
	processStarted  int
	endorseStarted  int
	submitStarted   int
	processFinished []error
	endorseFinished []error
	submitFinished  []error
}

func (o *recordingProcessObserver) ProcessStarted()     { o.processStarted++ }
func (o *recordingProcessObserver) EndorsementStarted() { o.endorseStarted++ }
func (o *recordingProcessObserver) EndorsementFinished(duration time.Duration, err error) {
	o.endorseFinished = append(o.endorseFinished, err)
}
func (o *recordingProcessObserver) SubmitStarted() { o.submitStarted++ }
func (o *recordingProcessObserver) SubmitFinished(duration time.Duration, err error) {
	o.submitFinished = append(o.submitFinished, err)
}
func (o *recordingProcessObserver) ProcessFinished(duration time.Duration, err error) {
	o.processFinished = append(o.processFinished, err)
}

func TestGatewayProcessTimingObserverSplitsEndorsementAndSubmit(t *testing.T) {
	ec, err := NewEndorsementClient([]Endorser{timingTestEndorser{}}, timingTestSigner{}, "mychannel", "0", "1")
	if err != nil {
		t.Fatal(err)
	}
	input := make(chan sdk.Endorsement, 1)
	g, err := New(ec, nil, nil, 4011, 1, nil, input)
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingProcessObserver{}
	g.SetProcessTimingObserver(observer)

	tx := types.NewTx(&types.LegacyTx{Nonce: 1})
	if err := g.processTx(context.Background(), tx); err != nil {
		t.Fatalf("processTx returned error: %v", err)
	}

	if observer.processStarted != 1 || observer.endorseStarted != 1 || observer.submitStarted != 1 {
		t.Fatalf("unexpected starts: process=%d endorse=%d submit=%d", observer.processStarted, observer.endorseStarted, observer.submitStarted)
	}
	if len(observer.endorseFinished) != 1 || observer.endorseFinished[0] != nil {
		t.Fatalf("endorse finish = %#v", observer.endorseFinished)
	}
	if len(observer.submitFinished) != 1 || observer.submitFinished[0] != nil {
		t.Fatalf("submit finish = %#v", observer.submitFinished)
	}
	if len(observer.processFinished) != 1 || observer.processFinished[0] != nil {
		t.Fatalf("process finish = %#v", observer.processFinished)
	}
}

type recordingSubmitObserver struct {
	calls int
	errs  []error
}

func (o *recordingSubmitObserver) BatchSubmitFinished(duration time.Duration, err error) {
	o.calls++
	o.errs = append(o.errs, err)
}

type timingSubmitter struct{ err error }

func (s timingSubmitter) Submit(context.Context, sdk.Endorsement) error { return s.err }
func (s timingSubmitter) Close() error                                  { return nil }

func TestBatchSubmitterTimingObserverRecordsActualSubmit(t *testing.T) {
	wantErr := errors.New("broadcast failed")
	observer := &recordingSubmitObserver{}
	bs := NewBatchSubmitter(timingSubmitter{err: wantErr}, nil, make(chan sdk.Endorsement, 1))
	bs.SetSubmitTimingObserver(observer)

	err := bs.submitOne(context.Background(), sdk.Endorsement{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("submitOne error = %v, want %v", err, wantErr)
	}
	if observer.calls != 1 {
		t.Fatalf("observer calls = %d, want 1", observer.calls)
	}
	if len(observer.errs) != 1 || !errors.Is(observer.errs[0], wantErr) {
		t.Fatalf("observer errors = %#v, want %v", observer.errs, wantErr)
	}
}
