package main

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
)

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher struct {
	results      map[string]*fakeResult
	callsCounter map[string]int
	m            *sync.Mutex
}

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	f.m.Lock()
	f.callsCounter[url]++
	f.m.Unlock()
	if res, ok := f.results[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

func TestCrawl(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		depths  int
		fetcher fakeFetcher
		result  []string
		err     error
	}{
		{
			name:   "0 depth",
			url:    "https://golang.org",
			depths: 0,
			fetcher: fakeFetcher{
				results: map[string]*fakeResult{"https://golang.org": {
					"should not be displayed",
					[]string{
						"https://golang.org",
						"https://golang.org/do-not-follow",
					},
				},
				},
				callsCounter: make(map[string]int),
				m:            &sync.Mutex{},
			},
			result: []string{},
			err:    nil,
		},
		{
			name:   "default",
			url:    "https://golang.org/",
			depths: 4,
			fetcher: fakeFetcher{
				results: map[string]*fakeResult{"https://golang.org/": &fakeResult{
					"The Go Programming Language",
					[]string{
						"https://golang.org/pkg/",
						"https://golang.org/cmd/",
					},
				},
					"https://golang.org/pkg/": &fakeResult{
						"Packages",
						[]string{
							"https://golang.org/",
							"https://golang.org/cmd/",
							"https://golang.org/pkg/fmt/",
							"https://golang.org/pkg/os/",
						},
					},
					"https://golang.org/pkg/fmt/": &fakeResult{
						"Package fmt",
						[]string{
							"https://golang.org/",
							"https://golang.org/pkg/",
						},
					},
					"https://golang.org/pkg/os/": &fakeResult{
						"Package os",
						[]string{
							"https://golang.org/",
							"https://golang.org/pkg/",
						},
					},
				},
				callsCounter: make(map[string]int),
				m:            &sync.Mutex{},
			},

			result: []string{
				"The Go Programming Language",
				"Packages",
				"Package fmt",
				"Package os",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Crawl(tt.url, tt.depths, tt.fetcher)
			if err != tt.err {
				t.Error(err)
			}
			sort.Strings(result)
			sort.Strings(tt.result)

			if !reflect.DeepEqual(tt.result, result) {
				t.Errorf("Wrong result. Expected: %+q, Got: %+q", tt.result, result)
			}

			fmt.Println(tt.fetcher.callsCounter)
		})
	}
}
