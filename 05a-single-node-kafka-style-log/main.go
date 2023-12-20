package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n       *maelstrom.Node
	logs    map[string][]logEntry
	offsets map[string]int
	mu      sync.RWMutex
}

type logEntry struct {
	offset int
	msg    int
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:       n,
		logs:    make(map[string][]logEntry),
		offsets: make(map[string]int),
	}

	n.Handle("send", s.sendHandler)
	n.Handle("poll", s.pollHandler)
	n.Handle("commit_offsets", s.commitOffsetsHandler)
	n.Handle("list_committed_offsets", s.listCommitedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type sendMsg struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body sendMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	offset := 0

	// get the last offset
	if len(s.logs[body.Key]) > 0 {
		offset = s.logs[body.Key][len(s.logs[body.Key])-1].offset + 1
	}

	s.logs[body.Key] = append(s.logs[body.Key], logEntry{
		offset: offset,
		msg:    body.Msg,
	})

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type offsetsMsg struct {
	Offsets map[string]int `json:"offsets"`
	Type    string         `json:"type"`
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var body offsetsMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := make(map[string][][]int)

	for key, offset := range body.Offsets {
		logs := s.logs[key]
		index := getOffsetIndex(logs, offset)

		for ; index < len(logs); index++ {
			entries := logs[index]
			intEntry := []int{entries.offset, entries.msg}
			msgs[key] = append(msgs[key], intEntry)
		}
	}

	res := map[string]any{
		"type": "poll_ok",
		"msgs": msgs,
	}

	return s.n.Reply(msg, res)
}

func (s *server) commitOffsetsHandler(msg maelstrom.Message) error {
	var body offsetsMsg
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for k, v := range body.Offsets {
		s.offsets[k] = v
	}

	res := map[string]any{
		"type": "commit_offsets_ok",
	}

	return s.n.Reply(msg, res)
}

func (s *server) listCommitedOffsetsHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	res := map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": s.offsets,
	}

	return s.n.Reply(msg, res)
}

func getOffsetIndex(entries []logEntry, startingOffset int) int {
	left, right := 0, len(entries)-1

	for left <= right {
		mid := left + (right-left)/2
		currentOffset := entries[mid].offset

		switch {
		case currentOffset == startingOffset:
			return mid
		case currentOffset < startingOffset:
			left = mid + 1
		default:
			right = mid - 1
		}
	}

	return left
}

// TODO: This is not efficient we should use a binary search
func (s *server) offsetIndex(logs []logEntry, offset int) int {
	log.Printf("offsetIndex logs: %v", logs)
	for i := range logs {
		log.Printf("offsetIndex offset: %v", offset)
		log.Printf("offsetIndex logs: %v", logs[i])
		if logs[i].offset == offset {
			log.Printf("found i: %v", i)
			return i
		}
	}

	return 0
}
