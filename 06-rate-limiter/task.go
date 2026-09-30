package main

import (
	"sync"
	"time"
)

type RateLimiter struct {
	capacity chan struct{}
	l        sync.Mutex
}

func NewRateLimiter(n int) *RateLimiter {
	capacity := make(chan struct{}, n-1)

	return &RateLimiter{
		capacity: capacity,
		l:        sync.Mutex{},
	}
}

func (r *RateLimiter) CanTake() bool {
	r.l.Lock()
	defer r.l.Unlock()
	return len(r.capacity) < cap(r.capacity)
}

func (r *RateLimiter) Take() {
	select {
	case r.capacity <- struct{}{}:
		go func() {
			tick := time.Tick(time.Second)
			<-tick
			<-r.capacity
		}()
	}
}
