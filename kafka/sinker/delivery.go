package sinker

import (
	"context"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	sink "github.com/streamingfast/substreams/sink"
	"go.uber.org/zap"
)

// 🚀 HIGH-PERFORMANCE ASYNC DELIVERY SYSTEM

func (s *KafkaSinker) addPendingMessage(key string, cursor *sink.Cursor) {
	pendingMutex.Lock()
	s.pendingMessages[key] = cursor
	pendingMutex.Unlock()
}

func (s *KafkaSinker) removePendingMessage(key string) *sink.Cursor {
	pendingMutex.Lock()
	defer pendingMutex.Unlock()
	cursor := s.pendingMessages[key]
	delete(s.pendingMessages, key)
	return cursor
}

func (s *KafkaSinker) deliveryConfirmationHandler(ctx context.Context) {
	s.logger.Info("🚀 Starting high-performance delivery confirmation handler")

	for {
		select {
		case event := <-s.deliveryChan:
			if msg, ok := event.(*kafka.Message); ok {
				// 🚀 OPTIMIZED: Use byte slice directly to avoid string allocation
				keyBytes := msg.Key
				key := string(keyBytes) // Only convert when needed for logging/map access

				if msg.TopicPartition.Error != nil {
					s.logger.Error("message delivery failed",
						zap.String("key", key),
						zap.Error(msg.TopicPartition.Error))
					// Remove failed message from pending
					s.removePendingMessage(key)
				} else {
					// Message successfully delivered!
					atomic.AddInt64(&s.messagesConfirmed, 1)
					atomic.AddInt64(&s.messagesPending, -1)

					// Update last confirmed cursor
					if cursor := s.removePendingMessage(key); cursor != nil {
						s.lastConfirmedCursor = cursor
					}

					// Notify cursor saver
					select {
					case s.confirmedMessages <- key:
					default:
						// Channel full, skip notification (cursor will be saved by timer)
					}
				}
			}

		case <-ctx.Done():
			s.logger.Info("delivery confirmation handler shutting down")
			return
		}
	}
}

func (s *KafkaSinker) cursorSaveHandler(ctx context.Context) {
	s.logger.Info("💾 Starting background cursor saver")

	for {
		select {
		case <-s.cursorSaveTicker.C:
			// Save cursor periodically
			cursorToSave := s.getCursorToSave()
			if cursorToSave != nil {
				if err := s.saveCursor(cursorToSave); err != nil {
					s.logger.Warn("failed to save cursor", zap.Error(err))
				}
			}

		case <-s.confirmedMessages:
			// Message confirmed, save cursor immediately for low latency
			cursorToSave := s.getCursorToSave()
			if cursorToSave != nil {
				if err := s.saveCursor(cursorToSave); err != nil {
					s.logger.Warn("failed to save cursor", zap.Error(err))
				}
			}

		case <-ctx.Done():
			s.logger.Info("cursor saver shutting down")
			// Final cursor save
			cursorToSave := s.getCursorToSave()
			if cursorToSave != nil {
				s.saveCursor(cursorToSave)
			}
			return
		}
	}
}

// getCursorToSave returns the appropriate cursor to save based on processing mode
func (s *KafkaSinker) getCursorToSave() *sink.Cursor {
	if s.undoBufferSize > 0 {
		// In undo buffer mode, save the last processed cursor (blocks sent to Kafka)
		return s.lastProcessedCursor
	} else {
		// In immediate mode, save the last confirmed cursor (original logic)
		return s.lastConfirmedCursor
	}
}
