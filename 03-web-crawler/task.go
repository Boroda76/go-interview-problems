package main

import (
	"errors"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) ([]string, error) {
	c := newCache()
	//assume that error should not drop whole crawl
	//todo: retry errors
	var err error
	urls := make(chan string)
	errs := make(chan error)
	go func() {
		_, init, e := fetcher.Fetch(url)
		if e != nil {
			errs <- e
			//safe since its inital call and if its fail no more attempts to send to this channel would happen
			close(urls)
			return
		}
		for _, url := range init {
			urls <- url
		}
	}()
	for {
		select {
		case u := <-urls:
			if c.set(u) {
				go fetch(fetcher, u, errs, urls)
			}
		case e := <-errs:
			err = errors.Join(err, e)
		default:
			close(urls)
			close(errs)
			return c.getUrls(), err
		}
	}

}

func fetch(f Fetcher, url string, errs chan error, urls chan string) {
	_, fetched, e := f.Fetch(url)
	if e != nil {
		errs <- e
		return
	}
	for _, u := range fetched {
		urls <- u
	}
}

type cache struct {
	urls map[string]any
	m    *sync.RWMutex
}

func newCache() *cache {
	return &cache{
		urls: make(map[string]any),
		m:    &sync.RWMutex{},
	}
}

func (c *cache) getUrls() []string {
	var urls []string
	c.m.RLock()
	defer c.m.RUnlock()
	for k := range c.urls {
		urls = append(urls, k)
	}
	return urls
}

func (c *cache) get(url string) bool {
	c.m.RLock()
	defer c.m.RUnlock()
	_, ok := c.urls[url]
	return ok
}

func (c *cache) set(url string) bool {
	if c.get(url) {
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
