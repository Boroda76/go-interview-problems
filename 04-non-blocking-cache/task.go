package main

import "sync"

type Client interface {
	Get(address string) (string, error)
}

type cacheData struct {
	page string
	err  error
}
type cacheEntry struct {
	address string
	page    string
	err     error
	ready   chan cacheData
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

func (c *Cache) wait(address string) (string, error) {
	for range c.storage[address].ready {

	}
	return c.storage[address].page, c.storage[address].err
}

// Cache Client.Get result
func (c *Cache) Get(address string) (string, error) {
	//simple check
	c.l.RLock()
	if _, ok := c.storage[address]; ok {
		c.l.RUnlock()
		return c.wait(address)
	}
	c.l.RUnlock()

	c.l.Lock()
	if _, ok := c.storage[address]; ok {
		c.l.Unlock()
		return c.wait(address)
	}
	c.storage[address] = cacheEntry{ready: make(chan cacheData, 1)}
	c.l.Unlock()
	go func() {
		page, err := c.client.Get(address)
		v := c.storage[address]
		v.err = err
		v.page = page
		c.storage[address] = v
		c.storage[address].ready <- cacheData{page: page, err: err}
		close(c.storage[address].ready)
	}()

	return c.wait(address)
}
