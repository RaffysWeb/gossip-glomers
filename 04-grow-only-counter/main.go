package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const timeout = time.Second

type server struct {
	n     *maelstrom.Node
	kv    *maelstrom.KV
	store map[string]int

	mu sync.RWMutex
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)

	s := &server{
		n:     node,
		kv:    kv,
		store: make(map[string]int),
	}

	s.n.Handle("init", s.initHandler)
	s.n.Handle("add", s.addHandler)
	s.n.Handle("read", s.readHandler)
	s.n.Handle("getSum", s.getSumHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

func (s *server) initHandler(msg maelstrom.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.kv.Write(ctx, s.n.ID(), 0); err != nil {
		return err
	}

	res := map[string]any{
		"type": "init_ok",
	}

	return s.n.Reply(msg, res)
}

type addReq struct {
	Delta int `json:"delta"`
}

func (s *server) addHandler(msg maelstrom.Message) error {
	var body addReq
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()

	ctx, rCancel := context.WithTimeout(context.Background(), timeout)
	defer rCancel()

	sum, err := s.kv.ReadInt(ctx, s.n.ID())
	if err != nil {
		return err
	}

	err = s.kv.Write(ctx, s.n.ID(), sum+body.Delta)

	s.mu.Unlock()
	if err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

type readReq struct {
	Value int `json:"value"`
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body readReq
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	total := 0
	resultCh := make(chan int, len(s.n.NodeIDs()))
	errorCh := make(chan error, len(s.n.NodeIDs()))
	var wg sync.WaitGroup

	for _, nID := range s.n.NodeIDs() {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if nodeID == s.n.ID() {
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()

				v, err := s.kv.ReadInt(ctx, s.n.ID())
				if err != nil {
					errorCh <- err
					v = s.store[nodeID]
				}

				s.store[nodeID] = v
				resultCh <- v
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// If it's not the local node, try to fetch from the remote node
			res, err := s.n.SyncRPC(ctx, nodeID, map[string]any{
				"type": "getSum",
			})
			// If it fails, get from the local store
			if err != nil {
				errorCh <- err
				v, exists := s.store[nodeID]
				if !exists {
					s.store[nodeID] = 0
					resultCh <- 0
					return
				}

				resultCh <- v
				return
			}

			if err := json.Unmarshal(res.Body, &body); err != nil {
				errorCh <- err
				resultCh <- 0
				return
			}

			// Save in local cache
			s.store[nodeID] = body.Value
			resultCh <- body.Value
		}(nID)
	}

	wg.Wait()
	close(resultCh)
	close(errorCh)

	for v := range resultCh {
		total += v
	}

	for err := range errorCh {
		log.Printf("error: %v", err)
	}

	res := map[string]any{
		"type":  "read_ok",
		"value": total,
	}

	return s.n.Reply(msg, res)
}

func (s *server) getSumHandler(msg maelstrom.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	v, err := s.kv.ReadInt(ctx, s.n.ID())
	if err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type":  "getSum_ok",
		"value": v,
	})
}
