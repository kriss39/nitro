// Copyright 2025-2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
package melextraction

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/offchainlabs/nitro/arbnode/mel"
	"github.com/offchainlabs/nitro/solgen/go/bridgegen"
)

type EventUnpacker interface {
	UnpackLogTo(event any, abi *abi.ABI, eventName string, log types.Log) error
}

func ParseBatchesFromBlock(
	ctx context.Context,
	batchPostingTargetAddress common.Address,
	parentChainHeader *types.Header,
	txFetcher TransactionFetcher,
	logsFetcher LogsFetcher,
	eventUnpacker EventUnpacker,
) ([]*mel.SequencerInboxBatch, []*types.Transaction, error) {
	logs, err := logsFetcher.LogsForBlockHash(ctx, parentChainHeader.Hash())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch logs from parent chain block %v: %w", parentChainHeader.Hash(), err)
	}
	batches := make([]*mel.SequencerInboxBatch, 0, len(logs))
	batchTxs := make([]*types.Transaction, 0, len(logs))
	var lastSeqNum *uint64
	for _, log := range logs {
		// These logs are not necessarily pre-filtered. The replay binary's fetcher
		// hands over every log in the parent chain block, so a log may carry no
		// topics at all (an anonymous event) and must not be indexed into.
		if log == nil || len(log.Topics) == 0 || log.Topics[0] != BatchDeliveredID {
			continue
		}
		// Only the sequencer inbox can deliver a batch. Any other contract can emit a
		// log with the same signature, so the emitting address has to be checked.
		if log.Address != batchPostingTargetAddress {
			continue
		}
		event := new(bridgegen.SequencerInboxSequencerBatchDelivered)
		if err := eventUnpacker.UnpackLogTo(event, SeqInboxABI, "SequencerBatchDelivered", *log); err != nil {
			return nil, nil, err
		}
		if !event.BatchSequenceNumber.IsUint64() {
			return nil, nil, errors.New("sequencer inbox event has non-uint64 sequence number")
		}
		if !event.AfterDelayedMessagesRead.IsUint64() {
			return nil, nil, errors.New("sequencer inbox event has non-uint64 delayed messages read")
		}

		seqNum := event.BatchSequenceNumber.Uint64()
		if lastSeqNum != nil {
			if seqNum != *lastSeqNum+1 {
				return nil, nil, fmt.Errorf("sequencer batches out of order; after batch %v got batch %v", *lastSeqNum, seqNum)
			}
		}
		lastSeqNum = &seqNum

		tx, err := txFetcher.TransactionByLog(ctx, log)
		if err != nil {
			return nil, nil, fmt.Errorf("error fetching tx by hash: %v in ParseBatchesFromBlock: %w ", log.TxHash, err)
		}

		batch := &mel.SequencerInboxBatch{
			BlockHash:              log.BlockHash,
			ParentChainBlockNumber: log.BlockNumber,
			SequenceNumber:         seqNum,
			BeforeInboxAcc:         event.BeforeAcc,
			AfterInboxAcc:          event.AfterAcc,
			AfterDelayedAcc:        event.DelayedAcc,
			AfterDelayedCount:      event.AfterDelayedMessagesRead.Uint64(),
			RawLog:                 *log,
			TimeBounds:             event.TimeBounds,
			DataLocation:           mel.BatchDataLocation(event.DataLocation),
			BridgeAddress:          log.Address,
		}
		batches = append(batches, batch)
		batchTxs = append(batchTxs, tx)
	}
	return batches, batchTxs, nil
}
