package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	maxRetryCount = 100
	batchInterval = 200 * time.Millisecond
	nodeLeader    = "n0"
)

type server struct {
	n *maelstrom.Node

	messageIds   map[int]struct{}
	messageQueue map[int]struct{}
	nodeID       string
	initHandled  bool

	messageMu      sync.RWMutex
	messageQueueMu sync.RWMutex
	initLock       sync.Mutex
}

func main() {
	n := maelstrom.NewNode()

	s := &server{
		n:            n,
		messageIds:   make(map[int]struct{}),
		messageQueue: make(map[int]struct{}),
	}

	s.newBroadcastBatchWorker()
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type broadcastReq struct {
	Message   *int   `json:"message"`
	Type      string `json:"type"`
	Messages  []int  `json:"messages"`
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

	// Batch broadcasts don't have a message field
	if body.Message != nil {
		if s.messageExists(*body.Message) {
			return nil
		}

		s.storeMessage(*body.Message)
		return s.broadcast(msg.Src, body)
	}

	// messages should come from the leader and be unique
	if body.Messages != nil {
		for _, message := range body.Messages {
			s.storeMessage(message)
		}
	}

	return nil
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	res := map[string]any{
		"type":     "read_ok",
		"messages": s.storedMessages(),
	}
	return s.n.Reply(msg, res)
}

func (s *server) broadcast(srcId string, body broadcastReq) error {
	if srcId == nodeLeader {
		return nil
	}

	go func() {
		for retry := 0; retry < maxRetryCount; retry++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			// all nodes send to the leader
			_, err := s.n.SyncRPC(ctx, nodeLeader, body)

			if err == nil {
				break
			}

			time.Sleep(time.Duration(retry) * time.Second)
		}
	}()

	return nil
}

func (s *server) newBroadcastBatchWorker() {
	go func() {
		ticker := time.NewTicker(batchInterval)
		// create a for loop that only continues after s.n.ID() is not empty
		for s.n.ID() == "" {
			time.Sleep(50 * time.Millisecond)
		}

		// Only the node leader sends batches
		if s.n.ID() != nodeLeader {
			return
		}

		for range ticker.C {
			s.broadcastBatch()
		}
	}()
}

func (s *server) broadcastBatch() {
	//  TODO: move the getter to a separate function
	s.messageQueueMu.Lock()
	defer s.messageQueueMu.Unlock()

	queuedMessages := make([]int, 0, len(s.messageQueue))

	for id := range s.messageQueue {
		queuedMessages = append(queuedMessages, id)
	}

	// send batches to all children nodes
	for _, nodeID := range s.n.NodeIDs() {
		if nodeID == nodeLeader {
			continue
		}

		go func(queuedMessages []int, nodeID string) {
			body := map[string]any{
				"type":     "broadcast",
				"messages": queuedMessages,
			}

			for retry := 0; retry < maxRetryCount; retry++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				_, err := s.n.SyncRPC(ctx, nodeID, body)

				if err == nil {
					break
				}

				time.Sleep(time.Duration(retry) * time.Second)
			}
		}(queuedMessages, nodeID)
	}

	s.messageQueue = make(map[int]struct{})
}

func (s *server) storedMessages() []int {
	s.messageMu.RLock()
	defer s.messageMu.RUnlock()

	ids := make([]int, 0, len(s.messageIds))

	for id := range s.messageIds {
		ids = append(ids, id)
	}

	return ids
}

func (s *server) storeMessage(message int) {
	// add to the queue for the nodeLeader so it can send to the other nodes
	if s.n.ID() == nodeLeader {
		s.messageQueueMu.Lock()
		defer s.messageQueueMu.Unlock()

		s.messageQueue[message] = struct{}{}
	}

	s.messageMu.Lock()
	defer s.messageMu.Unlock()

	s.messageIds[message] = struct{}{}
}

func (s *server) messageExists(message int) bool {
	s.messageMu.RLock()
	defer s.messageMu.RUnlock()

	_, exists := s.messageIds[message]

	return exists
}
