package main

import "sync"

type Client interface {
	Get(address string) (string, error)
}

type cacheEntry struct {
	ready chan struct{}
	page  string
	err   error
}
type Cache struct {
	client Client
	// You can add new fields if needed
	storage map[string]*cacheEntry
	l       sync.Mutex
}

// Don't update signature of NewCache
func NewCache(client Client) *Cache {
	return &Cache{client: client, storage: make(map[string]*cacheEntry), l: sync.Mutex{}}
}

// Cache Client.Get result
func (c *Cache) Get(address string) (string, error) {
	c.l.Lock()
	entry := c.storage[address]
	if entry == nil {
		entry = &cacheEntry{ready: make(chan struct{})}
		c.storage[address] = entry
		c.l.Unlock()
		entry.page, entry.err = c.client.Get(address)
		close(entry.ready)
	} else {
		c.l.Unlock()
		<-entry.ready
	}
	return entry.page, entry.err
}
