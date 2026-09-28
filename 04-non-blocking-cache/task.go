package main

import "sync"

type Client interface {
	Get(address string) (string, error)
}

type cacheEntry struct {
	address string
	page    string
	err     error
}
type Cache struct {
	client Client
	// You can add new fields if needed
	storage map[string]cacheEntry
	l       *sync.RWMutex
}

// Don't update signature of NewCache
func NewCache(client Client) *Cache {
	storage := make(map[string]cacheEntry)
	l := sync.RWMutex{}
	return &Cache{client: client, storage: storage, l: &l}
}

// Cache Client.Get result
func (c *Cache) Get(address string) (string, error) {
	c.l.RLock()
	if entry, ok := c.storage[address]; ok {
		c.l.RUnlock()
		return entry.page, entry.err
	}
	c.l.RUnlock()
	c.l.Lock()
	if entry, ok := c.storage[address]; ok {
		c.l.Unlock()
		return entry.page, entry.err
	}
	page, err := c.client.Get(address)
	c.storage[address] = cacheEntry{address: address, page: page, err: err}
	c.l.Unlock()
	return page, err
}
