package shortid

import (
	"context"
	"sync"
)

// BlockSource reserves a contiguous block of IDs and returns its first ID.
// The Postgres implementation is a sequence with INCREMENT BY blockSize, so a
// single nextval() reserves blockSize IDs for this process.
type BlockSource interface {
	NextBlock(ctx context.Context) (uint64, error)
}

// Allocator hands out unique IDs from blocks reserved in a shared BlockSource.
// One database round trip serves blockSize creates; different instances never
// receive overlapping blocks. IDs left unused when a process exits are skipped,
// which is harmless.
type Allocator struct {
	src       BlockSource
	blockSize uint64

	mu   sync.Mutex
	next uint64
	end  uint64
}

func NewAllocator(src BlockSource, blockSize uint64) *Allocator {
	return &Allocator{src: src, blockSize: blockSize}
}

func (a *Allocator) Next(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.next >= a.end {
		start, err := a.src.NextBlock(ctx)
		if err != nil {
			return 0, err
		}
		a.next, a.end = start, start+a.blockSize
	}
	id := a.next
	a.next++
	return id, nil
}
