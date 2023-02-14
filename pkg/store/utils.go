package store

import (
	"time"

	"go.uber.org/zap"
)

// collectChunkUploadError collects the error from all go routine to upload individual chunks
func collectChunkUploadError(chunkUploadCh chan<- chunk, resCh <-chan chunkUploadResult, stopCh chan struct{}, noOfChunks int64) *chunkUploadResult {
	remainingChunks := noOfChunks
	zap.S().Infof("No of Chunks:= %d", noOfChunks)
	for chunkRes := range resCh {
		zap.S().Infof("Received chunk result for id: %d, offset: %d", chunkRes.chunk.id, chunkRes.chunk.offset)
		if chunkRes.err != nil {
			zap.S().Infof("Chunk upload failed for id: %d, offset: %d with err: %v", chunkRes.chunk.id, chunkRes.chunk.offset, chunkRes.err)
			if chunkRes.chunk.attempt == maxRetryAttempts {
				zap.S().Errorf("Received the chunk upload error even after %d attempts from one of the workers. Sending stop signal to all workers.", chunkRes.chunk.attempt)
				close(stopCh)
				return &chunkRes
			}
			chunk := chunkRes.chunk
			delayTime := 1 << chunk.attempt
			chunk.attempt++
			zap.S().Warnf("Will try to upload chunk id: %d, offset: %d at attempt %d  after %d seconds", chunk.id, chunk.offset, chunk.attempt, delayTime)
			time.AfterFunc(time.Duration(delayTime)*time.Second, func() {
				select {
				case <-stopCh:
					return
				default:
					chunkUploadCh <- *chunk
				}
			})
		} else {
			remainingChunks--
			if remainingChunks == 0 {
				zap.S().Infof("Received successful chunk result for all chunks. Stopping workers.")
				close(stopCh)
				break
			}
		}
	}
	return nil
}
