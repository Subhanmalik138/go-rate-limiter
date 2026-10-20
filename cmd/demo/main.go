// Command demo simulates two API keys hammering a shared rate limiter and
// shows how the bucket, reserve API, and per-key registry behave.
package main

import (
	"fmt"
	"time"

	"github.com/Subhanmalik138/go-rate-limiter/ratelimit"
)

func main() {
	// 5 requests burst, then 1 per second — per API key.
	reg := ratelimit.NewRegistry(5, 1, 30*time.Second)
	defer reg.Close()

	keys := []string{"key-alice", "key-bob"}
	fmt.Println("Burst phase: each key fires 8 rapid requests (bucket holds 5)")
	for i := 0; i < 8; i++ {
		for _, k := range keys {
			status := "denied"
			if reg.Allow(k) {
				status = "allowed"
			}
			fmt.Printf("  %-10s request %d -> %s\n", k, i+1, status)
		}
	}

	fmt.Println("\nRecovery phase: wait 2.5s, each key fires 3 requests")
	time.Sleep(2500 * time.Millisecond)
	for i := 0; i < 3; i++ {
		for _, k := range keys {
			status := "denied"
			if reg.Allow(k) {
				status = "allowed"
			}
			fmt.Printf("  %-10s request %d -> %s\n", k, i+1, status)
		}
	}

	fmt.Println("\nReserve phase: how long must key-alice wait for 5 tokens?")
	wait, ok := reg.Get("key-alice").Reserve(5)
	fmt.Printf("  wait=%v ok=%v\n", wait.Round(time.Millisecond), ok)

	if wait, ok := reg.Get("key-alice").Reserve(999); !ok {
		fmt.Println("  reserving 999 tokens (over capacity) is impossible -> rejected, bucket untouched")
		_ = wait
	}
}
