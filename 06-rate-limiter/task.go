package main

import (
	"sync"
	"time"
)

type RateLimiter struct {
	n        int
	capacity chan struct{}
	l        sync.Mutex
}

func NewRateLimiter(n int) *RateLimiter {
	capacity := make(chan struct{}, n-1)

	return &RateLimiter{
		n:        n,
		capacity: capacity,
		l:        sync.Mutex{},
	}
}

func (r *RateLimiter) CanTake() bool {
	select {
	case r.capacity <- struct{}{}:
		tick := time.Tick(time.Second / time.Duration(r.n))
		<-tick
		<-r.capacity
		return true
	default:
		return false
	}

}

func (r *RateLimiter) Take() {
	select {
	case r.capacity <- struct{}{}:
		go func() {
			tick := time.Tick(time.Second / time.Duration(r.n))
			<-tick
			<-r.capacity
		}()
	}
}
