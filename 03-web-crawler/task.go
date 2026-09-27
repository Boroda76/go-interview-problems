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

type task struct {
	url   string
	depth int
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) ([]string, error) {
	if depth == 0 {
		return []string{}, nil
	}
	done := make(chan struct{})
	var counter atomic.Int64
	var bodies []string
	counter.Store(1)
	errc := make(chan error)
	urlsc := make(chan task)
	bodiesc := make(chan string)
	var err error
	urlsCache := newCache()

	go fetch(url, 0, depth, fetcher, urlsc, bodiesc, urlsCache, &counter, errc, done)

	for counter.Load() > 0 {
		select {
		case e := <-errc:
			err = errors.Join(err, e)
		case u := <-urlsc:
			go fetch(u.url, u.depth, depth, fetcher, urlsc, bodiesc, urlsCache, &counter, errc, done)
		case <-done:
			counter.Add(-1)
		case b := <-bodiesc:
			bodies = append(bodies, b)
		}
	}
	fmt.Printf("error is: %v\n", err)
	return bodies, nil
}

func fetch(url string, depth, maxDepth int, f Fetcher, urlsc chan task, bodiesc chan<- string, c *cache, counter *atomic.Int64, errc chan<- error, done chan<- struct{}) {
	defer func() {
		done <- struct{}{}
	}()
	if depth >= maxDepth {
		return
	}
	//if set returned true this means it is a first time url fetched
	cv, ok := c.get(url)
	if !ok {
		cv = c.setUrl(url, depth)
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
		c.setUrls(url, urls)

		bodiesc <- body

		for _, u := range urls {
			counter.Add(1)
			urlsc <- task{u, cv.minDepth + 1}
		}
	} else {
		if cv.minDepth > depth {
			c.setUrl(url, depth)

			cv, _ = c.get(url)
			for _, u := range cv.urls {
				counter.Add(1)
				urlsc <- task{u, cv.minDepth + 1}
			}
		}
	}

}

type cacheValue struct {
	urls     []string
	minDepth int
}
type cache struct {
	urls map[string]cacheValue
	m    *sync.RWMutex
}

func newCache() *cache {
	return &cache{
		urls: make(map[string]cacheValue),
		m:    &sync.RWMutex{},
	}
}

/*
cache operations:
1. set just url + depth
 (solve min depth inside cache)
2. set url + urls + depth
 (solve min depth, solve nil urls a not yet, empty as empty)
3. get urls + depth
*/

func (c *cache) setUrl(url string, depth int) cacheValue {
	c.m.Lock()
	defer c.m.Unlock()
	if v, ok := c.urls[url]; ok {
		v.minDepth = min(depth, v.minDepth)
		c.urls[url] = v
		return v
	}
	v := cacheValue{
		minDepth: depth,
	}
	c.urls[url] = v
	return v
}

func (c *cache) setUrls(url string, urls []string) cacheValue {
	c.m.Lock()
	defer c.m.Unlock()
	if v, ok := c.urls[url]; ok {
		if v.urls != nil {
			return v
		}
		v.urls = urls
		c.urls[url] = v
		return v
	}
	panic("unexpected cache state")
}

func (c *cache) get(url string) (cacheValue, bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	if v, ok := c.urls[url]; ok {
		return v, true
	}
	return cacheValue{}, false
}
