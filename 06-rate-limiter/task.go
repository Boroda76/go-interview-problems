package main

import (
	"time"
)

type RateLimiter struct {
	n      int
	bucket chan struct{}
}

func NewRateLimiter(n int) *RateLimiter {
	capacity := make(chan struct{}, n)

	//tick n times per second
	ticker := time.NewTicker(time.Second / time.Duration(n))

	for i := 0; i < n; i++ {
		capacity <- struct{}{}
	}

	go func() {
		for {
			select {
			case <-ticker.C:
				//empty 1 slot or do nothing
				select {
				case <-capacity:
				default:
					continue
				}
			}
		}
	}()

	return &RateLimiter{
		n:      n,
		bucket: capacity,
	}
}

func (r *RateLimiter) CanTake() bool {
	select {
	case r.bucket <- struct{}{}:
		return true
	default:
		return false
	}

}

func (r *RateLimiter) Take() {
	select {
	case r.bucket <- struct{}{}:
	}
}
