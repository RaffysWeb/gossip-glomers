package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const maxRetryCount = 100

type server struct {
	n        *maelstrom.Node
	topology map[string][]string

	messageIds map[int]struct{}
	mu         sync.RWMutex
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, messageIds: make(map[int]struct{})}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type broadcastReq struct {
	Type      string `json:"type"`
	Message   int    `json:"message"`
	MessageID int    `json:"msg_id"`
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body broadcastReq
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go func() {
		res := map[string]any{
			"type": "broadcast_ok",
		}

		s.n.Reply(msg, res)
	}()

	if s.messageExists(body.Message) {
		return nil
	}

	s.storeMessage(body.Message)

	return s.broadcast(msg.Src, body)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	res := map[string]any{
		"type":     "read_ok",
		"messages": s.storedMessages(),
	}
	return s.n.Reply(msg, res)
}

type topologyReq struct {
	Topology map[string][]string `json:"topology"`
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body topologyReq
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.topology = body.Topology

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (s *server) broadcast(srcId string, body broadcastReq) error {
	for _, id := range s.topology[s.n.ID()] {
		go func(id string) {
			for retry := 0; retry < maxRetryCount; retry++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				_, err := s.n.SyncRPC(ctx, id, body)

				if err == nil {
					break
				}

				time.Sleep(time.Duration(retry) * time.Second)
			}
		}(id)
	}
	return nil
}

func (s *server) storedMessages() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]int, 0, len(s.messageIds))

	for id := range s.messageIds {
		ids = append(ids, id)
	}

	return ids
}

func (s *server) storeMessage(message int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.messageIds[message] = struct{}{}
}

func (s *server) messageExists(message int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.messageIds[message]
	return exists
}
