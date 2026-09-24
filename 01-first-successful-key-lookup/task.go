package main

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
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
	group, wgCtx := errgroup.WithContext(ctx)
	response := make(chan string, 1)

	call := func(ctx context.Context, address, key string, response chan<- string) error {
		resp, err := getter.Get(ctx, address, key)
		if err != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case response <- resp:
			return errors.New("done")
		}

	}
	for _, address := range addresses {
		group.Go(func() error {
			return call(wgCtx, address, key, response)
		})
	}

	err := group.Wait()
	if err != nil {
		close(response)
		return <-response, nil
	}

	return "", errors.New("not found")
}
