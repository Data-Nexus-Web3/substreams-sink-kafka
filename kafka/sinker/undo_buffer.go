package sinker

import (
	"context"
	"sync"
	"time"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// BufferedBlock represents a block that is being held in the undo buffer
type BufferedBlock struct {
	Data      *pbsubstreamsrpc.BlockScopedData
	IsLive    *bool
	Cursor    *sink.Cursor
	BlockNum  uint64
	Timestamp time.Time
}

// UndoBuffer manages blocks that are held until they are confirmed by N subsequent blocks
type UndoBuffer struct {
	blocks         []BufferedBlock
	bufferSize     int
	currentHeadNum uint64
	mutex          sync.RWMutex
	logger         *zap.Logger
	sinker         *KafkaSinker

	// Cursor tracking
	lastReceivedCursor  *sink.Cursor
	lastProcessedCursor *sink.Cursor

	// Metrics
	blocksBuffered  int64
	blocksReleased  int64
	blocksDiscarded int64
}

// NewUndoBuffer creates a new undo buffer
func NewUndoBuffer(bufferSize int, sinker *KafkaSinker, logger *zap.Logger) *UndoBuffer {
	return &UndoBuffer{
		blocks:     make([]BufferedBlock, 0),
		bufferSize: bufferSize,
		logger:     logger,
		sinker:     sinker,
	}
}

// AddBlock adds a block to the buffer and releases any blocks that are ready
func (ub *UndoBuffer) AddBlock(data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	ub.mutex.Lock()
	defer ub.mutex.Unlock()

	blockNum := data.Clock.Number

	// Update current head
	if blockNum > ub.currentHeadNum {
		ub.currentHeadNum = blockNum
	}

	// Update received cursor
	ub.lastReceivedCursor = cursor
	ub.sinker.lastReceivedCursor = cursor

	// Add block to buffer
	bufferedBlock := BufferedBlock{
		Data:      data,
		IsLive:    isLive,
		Cursor:    cursor,
		BlockNum:  blockNum,
		Timestamp: time.Now(),
	}

	ub.blocks = append(ub.blocks, bufferedBlock)
	ub.blocksBuffered++

	ub.logger.Debug("block added to undo buffer",
		zap.Uint64("block_num", blockNum),
		zap.Uint64("current_head", ub.currentHeadNum),
		zap.Int("buffer_size", len(ub.blocks)),
	)

	// Check if any blocks can be released
	return ub.releaseReadyBlocks()
}

// ShouldReleaseBlock determines if a block should be released from the buffer
func (ub *UndoBuffer) ShouldReleaseBlock(blockNum uint64) bool {
	// Only release blocks that are bufferSize blocks behind current head
	return ub.currentHeadNum >= blockNum+uint64(ub.bufferSize)
}

// releaseReadyBlocks releases blocks that have been confirmed by enough subsequent blocks
func (ub *UndoBuffer) releaseReadyBlocks() error {
	var toRelease []BufferedBlock
	var remaining []BufferedBlock

	for _, block := range ub.blocks {
		if ub.ShouldReleaseBlock(block.BlockNum) {
			toRelease = append(toRelease, block)
		} else {
			remaining = append(remaining, block)
		}
	}

	ub.blocks = remaining

	if len(toRelease) > 0 {
		ub.logger.Debug("releasing blocks from undo buffer",
			zap.Int("blocks_to_release", len(toRelease)),
			zap.Int("blocks_remaining", len(remaining)),
			zap.Uint64("current_head", ub.currentHeadNum),
		)
	}

	// Process released blocks in order
	for _, block := range toRelease {
		if err := ub.processBlock(block); err != nil {
			ub.logger.Error("failed to process released block",
				zap.Uint64("block_num", block.BlockNum),
				zap.Error(err),
			)
			return err
		}

		// Update processed cursor to the released block's cursor
		ub.lastProcessedCursor = block.Cursor
		ub.sinker.lastProcessedCursor = block.Cursor
		ub.blocksReleased++
	}

	return nil
}

// processBlock processes a block through the original pipeline
func (ub *UndoBuffer) processBlock(block BufferedBlock) error {
	// Route to the appropriate processing method based on explosion configuration
	if ub.sinker.explodeFieldName != "" {
		return ub.sinker.handleWithFieldExplosion(context.Background(), block.Data, block.IsLive, block.Cursor)
	}
	return ub.sinker.handleWithoutExplosion(context.Background(), block.Data, block.IsLive, block.Cursor)
}

// HandleUndo processes an undo signal by discarding invalidated blocks
func (ub *UndoBuffer) HandleUndo(undoSignal *pbsubstreamsrpc.BlockUndoSignal) {
	ub.mutex.Lock()
	defer ub.mutex.Unlock()

	lastValidBlockNum := undoSignal.LastValidBlock.Number

	// Remove all buffered blocks with block numbers > lastValidBlockNum
	var validBlocks []BufferedBlock
	discardedCount := 0

	for _, block := range ub.blocks {
		if block.BlockNum <= lastValidBlockNum {
			validBlocks = append(validBlocks, block)
		} else {
			discardedCount++
			ub.blocksDiscarded++
		}
	}

	ub.blocks = validBlocks

	// Reset current head to the last valid block
	if lastValidBlockNum < ub.currentHeadNum {
		ub.currentHeadNum = lastValidBlockNum
	}

	ub.logger.Info("discarded buffered blocks due to reorg",
		zap.Int("discarded_count", discardedCount),
		zap.Uint64("last_valid_block", lastValidBlockNum),
		zap.Int("remaining_buffered", len(ub.blocks)),
		zap.Uint64("new_head", ub.currentHeadNum),
	)
}

// GetStats returns buffer statistics
func (ub *UndoBuffer) GetStats() (buffered, released, discarded int64, bufferSize int) {
	ub.mutex.RLock()
	defer ub.mutex.RUnlock()

	return ub.blocksBuffered, ub.blocksReleased, ub.blocksDiscarded, len(ub.blocks)
}

// GetCurrentBufferSize returns the current number of blocks in the buffer
func (ub *UndoBuffer) GetCurrentBufferSize() int {
	ub.mutex.RLock()
	defer ub.mutex.RUnlock()

	return len(ub.blocks)
}

// Close cleans up the undo buffer
func (ub *UndoBuffer) Close() error {
	ub.mutex.Lock()
	defer ub.mutex.Unlock()

	buffered, released, discarded, currentSize := ub.GetStats()

	ub.logger.Info("closing undo buffer",
		zap.Int64("total_blocks_buffered", buffered),
		zap.Int64("total_blocks_released", released),
		zap.Int64("total_blocks_discarded", discarded),
		zap.Int("blocks_remaining", currentSize),
	)

	// Clear the buffer
	ub.blocks = nil

	return nil
}
