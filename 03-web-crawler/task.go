package main

import (
	"errors"
	"fmt"
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
	done := make(chan struct{})
	var counter atomic.Int64
	var bodies []string
	counter.Store(1)
	errc := make(chan error)
	urlsc := make(chan string)
	bodiesc := make(chan string)
	var err error
	urlsCache := newCache()

	go fetch(url, fetcher, urlsc, bodiesc, urlsCache, &counter, errc, done)

	for counter.Load() > 0 {
		select {
		case e := <-errc:
			err = errors.Join(err, e)
		case u := <-urlsc:
			go fetch(u, fetcher, urlsc, bodiesc, urlsCache, &counter, errc, done)
		case <-done:
			counter.Add(-1)
		case b := <-bodiesc:
			bodies = append(bodies, b)
		}
	}
	fmt.Printf("error is: %v\n", err)
	return bodies, nil
}

func fetch(url string, f Fetcher, urlsc, bodiesc chan<- string, c *cache, counter *atomic.Int64, errc chan<- error, done chan<- struct{}) {
	defer func() {
		done <- struct{}{}
	}()
	//if set returned true this means it is a first time url fetched
	if c.set(url) {
		body, urls, err := f.Fetch(url)
		if err != nil {
			//up to 3 retries
			for i := 0; i < 2 && err != nil; i++ {
				body, urls, err = f.Fetch(url)
			}
			if err != nil {
				errc <- err
				return
			}
		}

		bodiesc <- body

		for _, u := range urls {
			counter.Add(1)
			urlsc <- u
		}
	}

}

type cache struct {
	urls map[string]bool
	m    *sync.RWMutex
}

func newCache() *cache {
	return &cache{
		urls: make(map[string]bool),
		m:    &sync.RWMutex{},
	}
}

func (c *cache) check(url string) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.urls[url]
	return ok
}

func (c *cache) set(url string) bool {
	if c.check(url) {
		return false
	}
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.urls[url]; !ok {
		c.urls[url] = true
		return true
	}
	return false
}
