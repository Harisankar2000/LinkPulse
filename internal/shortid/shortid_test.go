package shortid

import (
	"context"
	"sync"
	"testing"
)

func TestMultiplierIsCoprimeWithSpace(t *testing.T) {
	if multiplier%2 == 0 || multiplier%31 == 0 {
		t.Fatalf("multiplier %d shares a factor with 62^7", multiplier)
	}
	if offset >= Space {
		t.Fatalf("offset must be < Space")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []uint64{0, 1, 61, 62, 3843, 1_000_000, Space - 1}
	for _, n := range tests {
		code := Encode(n)
		if len(code) != Length {
			t.Fatalf("Encode(%d) = %q, want length %d", n, code, Length)
		}
		got, err := Decode(code)
		if err != nil || got != n {
			t.Fatalf("Decode(Encode(%d)) = %d, %v", n, got, err)
		}
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	for _, s := range []string{"", "abc", "abcdefgh", "abc-efg", "abc def"} {
		if _, err := Decode(s); err == nil {
			t.Errorf("Decode(%q) succeeded, want error", s)
		}
	}
}

func TestScrambleIsUniqueAndNonSequential(t *testing.T) {
	const n = 200_000
	seen := make(map[uint64]struct{}, n)
	var prev uint64
	sequentialPairs := 0
	for id := uint64(0); id < n; id++ {
		s, err := Scramble(id)
		if err != nil {
			t.Fatal(err)
		}
		if s >= Space {
			t.Fatalf("Scramble(%d) = %d out of range", id, s)
		}
		if _, dup := seen[s]; dup {
			t.Fatalf("collision at id %d", id)
		}
		seen[s] = struct{}{}
		if id > 0 && s == prev+1 {
			sequentialPairs++
		}
		prev = s
	}
	if sequentialPairs > 0 {
		t.Fatalf("found %d consecutive outputs; codes look guessable", sequentialPairs)
	}
}

func TestScrambleOutOfSpace(t *testing.T) {
	if _, err := Scramble(Space); err != ErrOutOfSpace {
		t.Fatalf("got %v, want ErrOutOfSpace", err)
	}
}

type fakeSource struct {
	mu    sync.Mutex
	next  uint64
	size  uint64
	calls int
}

func (f *fakeSource) NextBlock(context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	start := f.next
	f.next += f.size
	return start, nil
}

func TestAllocatorConcurrentUnique(t *testing.T) {
	src := &fakeSource{next: 1000, size: 100}
	a := NewAllocator(src, 100)

	const workers, perWorker = 50, 200
	var (
		mu   sync.Mutex
		seen = make(map[uint64]struct{}, workers*perWorker)
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id, err := a.Next(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id %d", id)
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if want := workers * perWorker / 100; src.calls != want {
		t.Fatalf("block fetches = %d, want %d", src.calls, want)
	}
}
