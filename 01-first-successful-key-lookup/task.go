package main

import (
	"context"
	"errors"
)

type Getter interface {
	Get(ctx context.Context, address, key string) (string, error)
}

// Call `Getter.Get()` for each address in parallel.
// Returns the first successful response.
// If all requests fail, returns an error.
func Get(ctx context.Context, getter Getter, addresses []string, key string) (string, error) {
	if len(addresses) == 0 {
		return "", nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	result := make(chan string, 1)
	failure := make(chan error, len(addresses))

	caller := func(ctx context.Context, addr string, key string, result chan string, failure chan error) {
		resp, err := getter.Get(ctx, addr, key)
		if err != nil {
			failure <- err
			return
		}
		select {
		case <-ctx.Done():
			return
		case result <- resp:
			return
		}
	}

	for _, address := range addresses {
		go caller(ctx, address, key, result, failure)
	}

	var err error
	var counter int

	for {
		if counter == len(addresses) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case e := <-failure:
			err = errors.Join(err, e)
			counter++
		case res := <-result:
			return res, nil
		}
	}
}
