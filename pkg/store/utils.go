package store

import (
	"crypto/sha256"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// collectChunkUploadError collects the error from all go routine to upload individual chunks
func collectChunkUploadError(chunkUploadCh chan<- chunk, resCh <-chan chunkUploadResult, stopCh chan struct{},
	noOfChunks int64, logger *zap.Logger) *chunkUploadResult {

	remainingChunks := noOfChunks
	logger.Info("Number of Chunks.", zap.Int64("total", noOfChunks))

	for chunkRes := range resCh {
		logger.Info("Received chunk result for id.", zap.Int("id", chunkRes.chunk.id), zap.Int64("offset", chunkRes.chunk.offset))
		if chunkRes.err != nil {
			logger.Info("Chunk upload failed for id.", zap.Int("id", chunkRes.chunk.id),
				zap.Int64("offset", chunkRes.chunk.offset), zap.NamedError("error", chunkRes.err))

			if chunkRes.chunk.attempt == maxRetryAttempts {
				logger.Error("Received the chunk upload error even after 5 attempts from one of the workers. Sending stop signal to all workers.")
				close(stopCh)
				return &chunkRes
			}
			chunk := chunkRes.chunk
			delayTime := 1 << chunk.attempt
			chunk.attempt++
			logger.Warn(fmt.Sprintf("Will try to upload chunk id: %d, offset: %d at attempt %d  after %d seconds", chunk.id, chunk.offset, chunk.attempt, delayTime))
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
				logger.Info("Received successful chunk result for all chunks. Stopping workers.")
				close(stopCh)
				break
			}
		}
	}
	return nil
}

func getHash(data interface{}) string {
	switch dat := data.(type) {
	case string:
		sha := sha256.Sum256([]byte(dat))
		return fmt.Sprintf("%x", sha)
	case []byte:
		sha := sha256.Sum256(dat)
		return fmt.Sprintf("%x", sha)
	}
	return ""
}
