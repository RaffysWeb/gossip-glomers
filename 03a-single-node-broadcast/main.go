package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n *maelstrom.Node

	messageIds []int
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type messageReq struct {
	Message int `json:"message"`
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body messageReq
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.messageIds = append(s.messageIds, body.Message)

	res := map[string]any{
		"type": "broadcast_ok",
	}

	return s.n.Reply(msg, res)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	res := map[string]any{
		"type":     "read_ok",
		"messages": s.messageIds,
	}
	return s.n.Reply(msg, res)
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	res := map[string]any{
		"type": "topology_ok",
	}

	return s.n.Reply(msg, res)
}
