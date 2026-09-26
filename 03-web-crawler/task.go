package main

import (
	"errors"
	"sync"
	"sync/atomic"
)

func main() {

}

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) ([]string, error) {
	var counter atomic.Int64
	counter.Store(1)
	errc := make(chan error)
	urlsc := make(chan string)
	var err error
	urlsCache := newCache()

	go fetch(url, fetcher, urlsc, urlsCache, &counter, errc)

	for counter.Load() > 0 {
		select {
		case e := <-errc:
			err = errors.Join(err, e)
		case u := <-urlsc:
			go fetch(u, fetcher, urlsc, urlsCache, &counter, errc)
		}
	}
	return urlsCache.getValues(), err
}

func fetch(url string, f Fetcher, urlsc chan<- string, c *cache, counter *atomic.Int64, errc chan<- error) {
	body, urls, err := f.Fetch(url)
	if err != nil {
		//up to 3 retries
		for i := 0; i < 2 || err == nil; i++ {
			body, urls, err = f.Fetch(url)
		}
		if err != nil {
			errc <- err
			counter.Add(-1)
			return
		}
	}
	//if set returned true this means it is a first time url fetched
	if c.set(url, body) {
		for _, u := range urls {
			counter.Add(1)
			urlsc <- u
		}
	} else {
		counter.Add(-1)
	}
}

type cache struct {
	urls map[string]string
	m    *sync.RWMutex
}

func newCache() *cache {
	return &cache{
		urls: make(map[string]string),
		m:    &sync.RWMutex{},
	}
}

func (c *cache) getValues() []string {
	var bodies []string
	c.m.RLock()
	defer c.m.RUnlock()
	for _, v := range c.urls {
		bodies = append(bodies, v)
	}
	return bodies
}

func (c *cache) check(url string) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.urls[url]
	return ok
}

func (c *cache) set(url, body string) bool {
	if c.check(url) {
		return false
	}
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.urls[url]; !ok {
		c.urls[url] = body
		return true
	}
	return false
}
