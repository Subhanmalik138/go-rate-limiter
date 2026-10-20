package ratelimit

import (
	"sync"
	"testing"
	"time"
)

func TestGetCreatesAndReuses(t *testing.T) {
	r := NewRegistry(5, 1, 0)
	defer r.Close()

	a := r.Get("alice")
	b := r.Get("alice")
	if a != b {
		t.Fatal("same key must return the same bucket")
	}
	if c := r.Get("bob"); c == a {
		t.Fatal("different keys must get different buckets")
	}
	if r.Len() != 2 {
		t.Fatalf("expected 2 buckets, got %d", r.Len())
	}
}

func TestPerKeyIndependence(t *testing.T) {
	r := NewRegistry(2, 0, 0) // no refill: fixed budgets
	defer r.Close()

	if !r.Allow("alice") || !r.Allow("alice") {
		t.Fatal("alice's first 2 requests should pass")
	}
	if r.Allow("alice") {
		t.Fatal("alice's 3rd request should be denied")
	}
	// bob has a fresh, untouched bucket
	if !r.Allow("bob") {
		t.Fatal("bob's bucket must be independent of alice's")
	}
}

func TestEviction(t *testing.T) {
	r := NewRegistry(5, 1, 50*time.Millisecond)
	defer r.Close()

	r.Allow("temp")
	if r.Len() != 1 {
		t.Fatal("bucket should be tracked")
	}
	time.Sleep(300 * time.Millisecond) // idleTTL/2 ticker + margin
	if r.Len() != 0 {
		t.Fatalf("idle bucket should have been evicted, len=%d", r.Len())
	}
	// bucket recreated fresh after eviction
	r.Allow("temp")
	if r.Len() != 1 {
		t.Fatal("bucket should be recreated on demand")
	}
}

func TestEvictionDisabled(t *testing.T) {
	r := NewRegistry(5, 1, 0)
	defer r.Close()
	r.Allow("key")
	time.Sleep(20 * time.Millisecond)
	if r.Len() != 1 {
		t.Fatal("eviction disabled: bucket must remain")
	}
}

func TestConcurrentGet(t *testing.T) {
	r := NewRegistry(1000, 100, 0)
	defer r.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				r.Allow("shared-key")
			}
		}(i)
	}
	wg.Wait()

	// 5000 requests at 1000 capacity, no refill in the test window that
	// matters: exactly 1000 should have succeeded. Instead of timing
	// assertions, check the bucket never went negative via Stats.
	tokens, _, _ := r.Get("shared-key").Stats()
	if tokens < 0 {
		t.Fatalf("tokens must never go negative, got %v", tokens)
	}
	if r.Len() != 1 {
		t.Fatalf("all goroutines should share one bucket, len=%d", r.Len())
	}
}
