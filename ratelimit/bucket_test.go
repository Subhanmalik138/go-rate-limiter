package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests advance time deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func newTestBucket(capacity, rate float64, c *fakeClock) *Bucket {
	b := New(capacity, rate)
	b.now = c.Now
	b.last = c.Now()
	return b
}

func TestBurstThenDeny(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(5, 1, c)

	for i := 0; i < 5; i++ {
		if !b.Allow() {
			t.Fatalf("burst token %d should be allowed", i)
		}
	}
	if b.Allow() {
		t.Fatal("6th token in burst should be denied")
	}
}

func TestRefillOverTime(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(10, 2, c) // 2 tokens/sec

	// drain
	for i := 0; i < 10; i++ {
		if !b.Allow() {
			t.Fatal("drain should succeed")
		}
	}
	if b.Allow() {
		t.Fatal("should be empty after drain")
	}

	c.Advance(1500 * time.Millisecond) // +3 tokens
	if !b.AllowN(3) {
		t.Fatal("should have refilled 3 tokens")
	}
	if b.Allow() {
		t.Fatal("only 3 tokens should have refilled")
	}
}

func TestRefillClampsAtCapacity(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(5, 100, c)
	if !b.AllowN(5) {
		t.Fatal("fresh bucket should allow 5")
	}
	c.Advance(time.Hour) // huge refill
	tokens, _, _ := b.Stats()
	if tokens != 5 {
		t.Fatalf("tokens should clamp at capacity 5, got %v", tokens)
	}
}

func TestOversizedRequestNeverAllowed(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(5, 10, c)
	if b.AllowN(6) {
		t.Fatal("request larger than capacity must be denied")
	}
	// bucket untouched: still full
	if tokens, _, _ := b.Stats(); tokens != 5 {
		t.Fatalf("denied request must not consume tokens, got %v", tokens)
	}
}

func TestReserve(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(5, 2, c) // 2 tokens/sec
	b.AllowN(5)                 // drain

	wait, ok := b.Reserve(3)
	if !ok {
		t.Fatal("reserve should be possible")
	}
	// deficit is 3 tokens at 2/sec -> 1.5s
	if wait < 1400*time.Millisecond || wait > 1600*time.Millisecond {
		t.Fatalf("expected ~1.5s wait, got %v", wait)
	}

	wait, ok = b.Reserve(1)
	if !ok || wait != 500*time.Millisecond {
		t.Fatalf("1 token at 2/sec needs 500ms: wait=%v ok=%v", wait, ok)
	}

	if _, ok := b.Reserve(6); ok {
		t.Fatal("reserving more than capacity must fail")
	}
}

func TestSetRateAndCapacity(t *testing.T) {
	c := &fakeClock{now: time.Now()}
	b := newTestBucket(10, 1, c)

	b.SetCapacity(3)
	if tokens, cap, _ := b.Stats(); tokens != 3 || cap != 3 {
		t.Fatalf("capacity shrink should clamp tokens: %v/%v", tokens, cap)
	}

	b.SetRate(10)
	c.Advance(100 * time.Millisecond) // +1 token at new rate, clamped to 3
	if tokens, _, rate := b.Stats(); tokens != 3 || rate != 10 {
		t.Fatalf("got tokens=%v rate=%v", tokens, rate)
	}
}

func TestZeroAndNegativeAllowN(t *testing.T) {
	b := New(5, 1)
	if !b.AllowN(0) || !b.AllowN(-2) {
		t.Fatal("non-positive requests should be trivially allowed")
	}
}

func TestPanicsOnBadConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity float64
		rate     float64
	}{
		{"zero capacity", 0, 1},
		{"negative capacity", -1, 1},
		{"negative rate", 5, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			New(tc.capacity, tc.rate)
		})
	}
}

// TestConcurrentAllow fires 200 goroutines at a capacity-10 bucket and
// asserts exactly 10 succeeded — no token may be spent twice.
func TestConcurrentAllow(t *testing.T) {
	b := New(10, 0) // no refill: exactly 10 tokens exist
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != 10 {
		t.Fatalf("expected exactly 10 allowed, got %d", allowed)
	}
}

// TestConcurrentMixedOps hammers a bucket with all operations; run with
// -race to verify there are no data races.
func TestConcurrentMixedOps(t *testing.T) {
	b := New(100, 50)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b.Allow()
			b.AllowN(2)
			b.Reserve(3)
			b.Stats()
			if i%10 == 0 {
				b.SetRate(60)
			}
		}(i)
	}
	wg.Wait()
}
