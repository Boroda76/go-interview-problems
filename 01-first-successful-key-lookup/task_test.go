package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Response represents a mock response with optional error and delay
type Response struct {
	Value string
	Error error
	Delay time.Duration
}

// MockGetter implements the Getter interface for testing
type MockGetter struct {
	Responses map[string]map[string]Response
	Waiters   chan struct{}
	Trigger   chan struct{}
	CtxErr    chan error
}

func NewMockGetter(responses map[string]map[string]Response, lenWaiters, lenTrigger uint, lenErrors int) *MockGetter {
	return &MockGetter{
		Responses: responses,
		Waiters:   make(chan struct{}, lenWaiters),
		Trigger:   make(chan struct{}, lenTrigger),
		CtxErr:    make(chan error, lenErrors),
	}
}

func (m *MockGetter) Get(ctx context.Context, address, key string) (string, error) {
	select {
	case m.Waiters <- struct{}{}:
	case <-ctx.Done():
		m.CtxErr <- ctx.Err()
		return "", ctx.Err()
	}
	select {
	case <-ctx.Done():
		m.CtxErr <- ctx.Err()
		return "", ctx.Err()
	case <-m.Trigger:
		if responses, exists := m.Responses[address]; exists {
			if resp, keyExists := responses[key]; keyExists {
				// Simulate delay if set
				if resp.Delay > 0 {
					select {
					case <-time.After(resp.Delay):
					case <-ctx.Done():
						m.CtxErr <- ctx.Err()
						return "", ctx.Err()
					}
				}

				// Return the error if set
				if resp.Error != nil {
					return "", resp.Error
				}

				// Return the response value
				return resp.Value, nil
			}
		}

		return "", errors.New("key not found")
	}
}

func TestGet(t *testing.T) {
	tests := []struct {
		name      string
		responses map[string]map[string]Response
		addresses []string
		key       string
		ttl       time.Duration
		wantValue string
		wantErr   bool
		// if these cfgs not set, default buffers set to the len(addresses) to avoid locks
		waitersAmount uint //  waitersAmount < len(addresses)
		aliveAmount   uint // aliveAmount < len(addresses); waitersAmount + aliveAmount <= len(addresses)
	}{
		{
			name: "first address fails second succeeds",
			responses: map[string]map[string]Response{
				"addr1": {
					"key1": {Error: errors.New("connection error")},
				},
				"addr2": {
					"key1": {Value: "value2"},
				},
			},
			addresses:     []string{"addr1", "addr2"},
			key:           "key1",
			wantValue:     "value2",
			ttl:           1 * time.Millisecond,
			wantErr:       false,
			waitersAmount: 2,
			aliveAmount:   2,
		},
		{
			name: "all addresses fail",
			responses: map[string]map[string]Response{
				"addr1": {
					"key1": {Error: errors.New("error 1")},
				},
				"addr2": {
					"key1": {Error: errors.New("error 2")},
				},
			},
			addresses:     []string{"addr1", "addr2"},
			key:           "key1",
			ttl:           1 * time.Millisecond,
			wantValue:     "",
			wantErr:       true,
			waitersAmount: 2,
			aliveAmount:   2,
		},
		{
			name: "context cancellation",
			responses: map[string]map[string]Response{
				"addr1": {
					"key1": {Value: "value1", Delay: 200 * time.Millisecond},
				},
			},
			addresses:     []string{"addr1"},
			key:           "key1",
			ttl:           50 * time.Millisecond,
			wantValue:     "",
			wantErr:       true,
			waitersAmount: 2,
			aliveAmount:   2,
		},
		{
			name: "fast address wins over slow",
			responses: map[string]map[string]Response{
				"addr1": {
					"key1": {Value: "value1", Delay: 200 * time.Millisecond},
				},
				"addr2": {
					"key1": {Value: "value2", Delay: 50 * time.Millisecond},
				},
			},
			addresses:     []string{"addr1", "addr2"},
			key:           "key1",
			ttl:           100 * time.Millisecond,
			wantValue:     "value2",
			wantErr:       false,
			waitersAmount: 2,
			aliveAmount:   2,
		},
		{
			name:      "empty address list",
			responses: map[string]map[string]Response{},
			addresses: []string{},
			key:       "key1",
			ttl:       50 * time.Millisecond,
			wantValue: "",
			wantErr:   false,
		},
		//case for reviewer 10 waits, 11th responses, 10 closes
		{
			name: "fast address wins over slow",
			responses: map[string]map[string]Response{
				"addr11": {
					"key1": {Value: "value11", Delay: 10 * time.Millisecond},
				},
			},
			addresses:     []string{"addr1", "addr2", "addr3", "addr4", "addr5", "addr6", "addr7", "addr8", "addr9", "addr10"},
			key:           "key1",
			ttl:           250 * time.Millisecond,
			wantValue:     "addr11",
			wantErr:       false,
			waitersAmount: 10,
			aliveAmount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockGetter(tt.responses, tt.waitersAmount, tt.aliveAmount, len(tt.addresses))

			var getter Getter
			if tt.name == "nil getter" {
				getter = nil
			} else {
				getter = mock
			}

			ctx, _ := context.WithTimeout(context.Background(), tt.ttl)

			go func() {
				got, err := Get(ctx, getter, tt.addresses, tt.key)
				if (err != nil) != tt.wantErr {
					t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if got != tt.wantValue {
					t.Errorf("Get() = %v, want %v", got, tt.wantValue)
				}
			}()

			var i uint
			for {
				select {
				case <-ctx.Done():
					return
				case <-mock.Waiters:
					if i++; i == tt.waitersAmount {
						mock.Trigger <- struct{}{}
					}
				case err := <-mock.CtxErr:
					//case for cancel check
					if errors.Is(err, context.Canceled) && ctx.Err() == nil {
					}
					//case for root cancel
					if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					}
					//case for ttl
					if errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
					}
				}
			}

		})
	}
}
