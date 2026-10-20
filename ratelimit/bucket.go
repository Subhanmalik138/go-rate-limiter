// Package ratelimit implements a thread-safe token bucket rate limiter.
//
// A token bucket holds up to `capacity` tokens and refills at a fixed rate.
// Every allowed action consumes tokens; when the bucket is empty, actions are
// denied (or must wait). Refills are computed lazily on every call, so the
// limiter costs O(1) per operation and needs no background ticker.
//
// All exported methods are safe for concurrent use.
package ratelimit

import (
	"sync"
	"time"
)

// Bucket is a thread-safe token bucket rate limiter.
type Bucket struct {
	mu         sync.Mutex
	capacity   float64 // maximum tokens the bucket can hold
	tokens     float64 // tokens currently available
	refillRate float64 // tokens added per second
	last       time.Time
	now        func() time.Time
}

// New returns a Bucket that holds up to `capacity` tokens and refills
// `refillRate` tokens per second. The bucket starts full.
//
// capacity must be > 0 and refillRate must be >= 0; New panics otherwise.
func New(capacity, refillRate float64) *Bucket {
	if capacity <= 0 {
		panic("ratelimit: capacity must be positive")
	}
	if refillRate < 0 {
		panic("ratelimit: refill rate cannot be negative")
	}
	return &Bucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		last:       time.Now(),
		now:        time.Now,
	}
}

// refill adds tokens earned since the last call. Callers must hold mu.
func (b *Bucket) refill() {
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.refillRate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.last = now
	}
}

// Allow reports whether one token is available, consuming it if so.
func (b *Bucket) Allow() bool {
	return b.AllowN(1)
}

// AllowN reports whether n tokens are available, consuming them if so.
// A request larger than capacity can never be satisfied and returns false
// without touching the bucket.
func (b *Bucket) AllowN(n float64) bool {
	if n <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if n > b.capacity {
		return false
	}
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// Reserve returns how long the caller must wait before n tokens will be
// available, without consuming anything. It reports ok=false when n exceeds
// capacity, meaning no amount of waiting would satisfy the request.
func (b *Bucket) Reserve(n float64) (wait time.Duration, ok bool) {
	if n <= 0 {
		return 0, true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if n > b.capacity {
		return 0, false
	}
	if b.tokens >= n {
		return 0, true
	}
	deficit := n - b.tokens
	seconds := deficit / b.refillRate
	return time.Duration(seconds * float64(time.Second)), true
}

// SetRate changes the refill rate (tokens per second) going forward.
func (b *Bucket) SetRate(refillRate float64) {
	if refillRate < 0 {
		panic("ratelimit: refill rate cannot be negative")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.refillRate = refillRate
}

// SetCapacity changes the bucket capacity, clamping stored tokens to it.
func (b *Bucket) SetCapacity(capacity float64) {
	if capacity <= 0 {
		panic("ratelimit: capacity must be positive")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	b.capacity = capacity
	if b.tokens > capacity {
		b.tokens = capacity
	}
}

// Stats returns a point-in-time snapshot of the bucket after refilling.
func (b *Bucket) Stats() (tokens, capacity, refillRate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens, b.capacity, b.refillRate
}
