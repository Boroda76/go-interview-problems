package main

import (
	"context"
	"sync"
	"time"
)

type TtlCache struct {
	l       sync.Mutex
	storage map[string]cacheVal
	//todo: re-build with built-in min heap
	//deleteQueue heap.Interface
	cancel context.CancelFunc
}

type cacheVal struct {
	value string
	valid int64
}

func (c *TtlCache) clear(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.l.Lock()
			for k, v := range c.storage {
				if time.Now().Unix() > v.valid {
					delete(c.storage, k)
				}
			}
			c.l.Unlock()
		}
	}
}

func NewTtlCache() *TtlCache {
	ctx, cancel := context.WithCancel(context.Background())

	cache := &TtlCache{
		storage: make(map[string]cacheVal),
		cancel:  cancel,
		l:       sync.Mutex{},
	}

	go cache.clear(ctx)

	return cache

}

func (c *TtlCache) Set(key string, value string, ttl time.Duration) {
	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}

	c.l.Lock()
	c.storage[key] = cacheVal{value, exp}
	c.l.Unlock()
}

func (c *TtlCache) Get(key string) (string, bool) {
	c.l.Lock()
	v, ok := c.storage[key]
	if ok {
		if time.Now().UnixNano() > v.valid && v.valid > 0 {
			delete(c.storage, key)
			c.l.Unlock()
			return "", false
		}
		c.l.Unlock()
		return v.value, true
	}
	c.l.Unlock()
	return "", false
}

func (c *TtlCache) Delete(key string) {
	c.l.Lock()
	delete(c.storage, key)
	c.l.Unlock()
}

func (c *TtlCache) Stop() {
	c.cancel()
}
