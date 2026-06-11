/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadReplayDatasetFromTSVGZGeneratesTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic_usdc_400k.tsv.gz")
	file, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	_, err = gz.Write([]byte("block_id\ttransaction_hash\ttime\ttoken_address\tsender\trecipient\tvalue\ttoken_name\ttoken_symbol\ttoken_decimals\n" +
		"2000000\t0x916e0fef6d7d657183f180475d8dbc521e77301d48da6c319fc3bfc0933de86d\t2020-01-01 00:00:00\t0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\t0xafdc112afced1a47834780eb743e4e21659a3cb2\t0xf22e64189bc9c108b2d1b9cd3f0f80ac07f3879a\t1000000\tUSD Coin\tUSDC\t6\n"))
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())

	transfers := loadReplayWindowFromPath(t, path, replayConfig{windowSize: 1})

	require.Len(t, transfers, 1)
	require.NotEmpty(t, transfers[0].Transaction)
}
