# go-rate-limiter

A thread-safe **token bucket** rate limiter in Go, with a per-key bucket
registry and idle eviction. Drop it in front of an API handler to cap bursts
and steady-state request rates per API key, IP address, or tenant.

## The problem

Naive rate limiting either blocks threads in `time.Sleep` loops or spins a
background ticker per client — both fall apart at scale. A token bucket gives
you both burst tolerance (a client may fire `capacity` requests at once) and a
sustainable rate (`refillRate` per second), computed lazily on every call in
O(1) time with no goroutines per client.

## Design decisions

1. **Lazy refill, no ticker.** Tokens are added on demand based on elapsed
   wall-clock time (`now.Sub(last) * rate`). Each bucket costs one mutex and
   zero goroutines, so ten thousand API keys don't mean ten thousand timers.
2. **Monotonic clock by default.** `time.Now()` includes the monotonic
   reading, so NTP jumps and daylight-saving changes can't grant or steal
   tokens. Tests inject a fake clock instead of sleeping.
3. **`Reserve` separates "when" from "whether".** It answers "how long until
   this request is servable?" without consuming tokens, so callers can
   schedule retries or queue precisely — and it returns `ok=false` for
   requests bigger than capacity instead of lying about an impossible wait.
4. **Registry evicts idle buckets.** The per-key map uses a single mutex
   (fine — critical sections are microseconds) plus one background goroutine
   that drops buckets idle past the TTL, so churn in keys can't grow memory
   forever. Eviction can be disabled with TTL ≤ 0.
5. **Lock granularity.** Each `Bucket` owns its own mutex; the registry lock
   is only held for map lookups, never during token math. Hot keys don't
   serialize against each other.

## Usage

```go
import "github.com/Subhanmalik138/go-rate-limiter/ratelimit"

// One bucket: burst of 10, then 5 requests/second.
b := ratelimit.New(10, 5)
if b.Allow() {
    // serve the request
}

// Per API key, evicting keys idle for a minute.
reg := ratelimit.NewRegistry(100, 10, time.Minute)
defer reg.Close()
if reg.Allow(r.Header.Get("X-API-Key")) {
    // serve the request
}

// How long until a big request is servable?
wait, ok := b.Reserve(8)
if !ok {
    // request can never fit the bucket — reject immediately
}
time.Sleep(wait)
```

## Run the demo

```bash
go run ./cmd/demo
```

It simulates two API keys bursting past their quota, recovering after a
pause, and using `Reserve` to schedule a large request.

## Run the tests

```bash
go test ./...          # unit tests
go test -race ./...    # also checks the concurrency tests for data races
go vet ./...
```

## API reference

| Method | Description |
|---|---|
| `New(capacity, rate)` | Bucket holding up to `capacity` tokens, refilling `rate`/sec. Starts full. |
| `Allow() / AllowN(n)` | Consume tokens if available; oversized requests are denied untouched. |
| `Reserve(n)` | `(wait, ok)` — wait until `n` tokens exist, without consuming. |
| `SetRate / SetCapacity` | Retune a live bucket; capacity shrink clamps stored tokens. |
| `Stats()` | Point-in-time snapshot after refill. |
| `NewRegistry(capacity, rate, idleTTL)` | Per-key buckets with idle eviction. |
| `Registry.Allow(key)` | One-token check for a key. |
| `Registry.Close()` | Stop the eviction goroutine. |

## Tech stack

- Go 1.23, standard library only (no dependencies)
- `sync.Mutex` concurrency control; monotonic clock timing
- Table-driven + clock-injected tests, `-race` verified
