package main

import (
	"context"
	"sync"
	"time"
)

type TtlCache struct {
	l       sync.Mutex
	storage map[string]task
	ctx     context.Context
	cancel  context.CancelFunc
	expire  chan string
}

type task struct {
	value string
	t     *time.Timer
}

func NewTtlCache() *TtlCache {
	c := new(TtlCache)
	c.storage = make(map[string]task)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *TtlCache) Set(key string, value string, ttl time.Duration) {
	c.l.Lock()
	if v, ok := c.storage[key]; ok {
		v.value = value
		if ttl > 0 {
			if v.t == nil {
				v.t = time.NewTimer(ttl)
			} else {
				v.t.Reset(ttl)
			}
			go func() {
				select {
				//ctx done has priority
				case <-c.ctx.Done():
					return
				default:
					select {
					case <-c.ctx.Done():
						return
					case <-v.t.C:
						c.Delete(key)
					}
				}
			}()
		} else {
			if v.t != nil {
				v.t.Stop()
			}
		}
		c.storage[key] = v
		c.l.Unlock()
		return
	}
	var t *time.Timer
	if ttl > 0 {
		t = time.NewTimer(ttl)
	}
	v := task{
		value: value,
		t:     t,
	}
	c.storage[key] = v
	if ttl > 0 {
		go func() {
			select {
			//ctx done has priority
			case <-c.ctx.Done():
				return
			default:
				select {
				case <-c.ctx.Done():
					return
				case <-v.t.C:
					c.Delete(key)
				}
			}
		}()
	}
	c.l.Unlock()
	return
}

func (c *TtlCache) Get(key string) (string, bool) {
	c.l.Lock()
	defer c.l.Unlock()
	v, ok := c.storage[key]
	if ok {
		return v.value, ok
	}

	return "", false
}

func (c *TtlCache) Delete(key string) {
	c.l.Lock()
	if v, ok := c.storage[key]; ok {
		if v.t != nil {
			v.t.Stop()
		}
		delete(c.storage, key)
	}
	c.l.Unlock()
}

func (c *TtlCache) Stop() {
	c.cancel()
}
