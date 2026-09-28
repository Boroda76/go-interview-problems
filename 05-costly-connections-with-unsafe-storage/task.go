package main

import "sync"

type Connection interface {
	// Need call Connect before Send
	// Take time to connect
	Connect()

	// Every connection should be disconnected after use
	// Take time to disconnect
	Disconnect()

	Send(req string) (string, error)
}

type ConnectionCreator interface {
	// Create new connection
	// Will return error if there is more than maxConn
	NewConnection() (Connection, error)
}

type Saver interface {
	// Saves data to unsafe storage
	// WILL CORRUPT DATA on concurrent save
	Save(data string)
}

// SendAndSave should send all requests concurrently using at most `maxConn` simultaneous connections.
// Responses must be saved using Saver.Save.
// Be careful: Saver.Save is not safe for concurrent use.
func SendAndSave(creator ConnectionCreator, saver Saver, requests []string, maxConn int) {
	//chan to deliver requests to the pool
	reqs := make(chan string)
	go func() {
		for _, req := range requests {
			reqs <- req
		}
		close(reqs)
	}()
	//channel for responses
	resps := make(chan string)
	defer close(resps)

	wg := new(sync.WaitGroup)

	go func() {
		for resp := range resps {
			saver.Save(resp)
		}
	}()

	for i := 0; i < maxConn; i++ {
		wg.Go(func() {
			//we can ignore an err since we open exactly maxConn amount of Connection instances
			c, _ := creator.NewConnection()

			//w8 for the connection established
			c.Connect()
			defer c.Disconnect()

			//once connection established we can start processing requests
			for r := range reqs {
				resp, err := c.Send(r)
				if err != nil {
					//todo: what to do with the errors? for now will just skip
					//we could retry them sending back to the reqs channel
					continue
				}
				resps <- resp
			}

			//once reqs closed we can close connection
		})
	}
	wg.Wait()
}
