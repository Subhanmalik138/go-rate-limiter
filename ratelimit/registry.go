// Registry keeps one token bucket per key (API key, IP address, tenant ID,
// ...), creating buckets on demand and evicting ones that have sat idle for
// longer than the configured TTL. All methods are safe for concurrent use.
package ratelimit

import (
	"sync"
	"time"
)

type entry struct {
	bucket   *Bucket
	lastUsed time.Time
}

// Registry maps keys to buckets. New buckets are built with the capacity and
// refill rate given to NewRegistry.
type Registry struct {
	mu       sync.Mutex
	buckets  map[string]*entry
	capacity float64
	rate     float64
	idleTTL  time.Duration

	stop chan struct{}
	done chan struct{}
}

// NewRegistry returns a Registry that creates buckets holding `capacity`
// tokens, refilling `rate` tokens per second. Buckets unused for `idleTTL`
// are evicted in the background. If idleTTL <= 0, eviction is disabled.
func NewRegistry(capacity, refillRate float64, idleTTL time.Duration) *Registry {
	r := &Registry{
		buckets:  make(map[string]*entry),
		capacity: capacity,
		rate:     refillRate,
		idleTTL:  idleTTL,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	if idleTTL > 0 {
		go r.evictLoop()
	} else {
		close(r.done)
	}
	return r
}

// Get returns the bucket for key, creating it on first use.
func (r *Registry) Get(key string) *Bucket {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.buckets[key]; ok {
		e.lastUsed = time.Now()
		return e.bucket
	}
	b := New(r.capacity, r.rate)
	r.buckets[key] = &entry{bucket: b, lastUsed: time.Now()}
	return b
}

// Allow reports whether the bucket for key has one token, consuming it.
func (r *Registry) Allow(key string) bool {
	return r.Get(key).Allow()
}

// AllowN reports whether the bucket for key has n tokens, consuming them.
func (r *Registry) AllowN(key string, n float64) bool {
	return r.Get(key).AllowN(n)
}

// Len reports how many buckets are currently tracked.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.buckets)
}

// Close stops the background eviction goroutine.
func (r *Registry) Close() {
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
	<-r.done
}

func (r *Registry) evictLoop() {
	defer close(r.done)
	ticker := time.NewTicker(r.idleTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.evict()
		}
	}
}

// evict drops buckets idle longer than idleTTL. Exported only for tests via
// the package-internal call.
func (r *Registry) evict() {
	cutoff := time.Now().Add(-r.idleTTL)
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, e := range r.buckets {
		if e.lastUsed.Before(cutoff) {
			delete(r.buckets, key)
		}
	}
}
