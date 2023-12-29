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
	n             *maelstrom.Node
	lKv           *maelstrom.KV
	sKv           *maelstrom.KV
	logs          map[string][]logEntry
	offsets       map[string]int
	latestOffsets map[string]int
	mu            sync.RWMutex
}

type logEntry struct {
	Offset int `json:"Offset"`
	Msg    int `json:"Msg"`
}

func main() {
	n := maelstrom.NewNode()
	lKv := maelstrom.NewLinKV(n)
	sKv := maelstrom.NewSeqKV(n)

	s := &server{
		n:   n,
		lKv: lKv,
		sKv: sKv,

		logs:          make(map[string][]logEntry),
		offsets:       make(map[string]int),
		latestOffsets: make(map[string]int),
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

	ctx, Cancel := context.WithTimeout(context.Background(), timeout)
	defer Cancel()

	// Get the latest offset for the key
	offset, err := s.sKv.ReadInt(ctx, body.Key)

	if err != nil {
		offset = 0
	} else {
		// we should check if the error is from the key not found or generic
		offset += 1
	}

	// we could have a local cache and if the latest offset in the cache
	// is the same we currently have, we don't need to read

	// Get the logs from the store

	// mockLogs := []logEntry{
	// 	{Offset: 0, Msg: 1},
	// 	{Offset: 1, Msg: 2},
	// 	{Offset: 2, Msg: 3},
	// }
	//
	// value, err := json.Marshal(mockLogs)
	// if err != nil {
	// 	return err
	// }
	//
	// err = s.lKv.Write(ctx, "test", string(value))
	//
	// if err != nil {
	// 	return err
	// }

	logs, err := s.lKv.Read(context.Background(), body.Key)
	if err != nil {
		// When trying to read before writing, we get an error
		log.Printf("err: %v", err)
		logs = `[]`
	}

	var logEntries []logEntry
	if logsStr, ok := logs.(string); ok {
		logsData := []byte(logsStr)
		if err := json.Unmarshal(logsData, &logEntries); err != nil {
			return err
		}
	}

	logEntries = append(logEntries, logEntry{
		Offset: offset,
		Msg:    body.Msg,
	})

	entries, err := json.Marshal(logEntries)
	if err != nil {
		return err
	}

	err = s.lKv.Write(ctx, body.Key, string(entries))

	if err != nil {
		return err
	}

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
			intEntry := []int{entries.Offset, entries.Msg}
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

// TODO: This is not efficient we should use a binary search
func (s *server) offsetIndex(logs []logEntry, offset int) int {
	log.Printf("offsetIndex logs: %v", logs)
	for i := range logs {
		log.Printf("offsetIndex offset: %v", offset)
		log.Printf("offsetIndex logs: %v", logs[i])
		if logs[i].Offset == offset {
			log.Printf("found i: %v", i)
			return i
		}
	}

	return 0
}

func getOffsetIndex(entries []logEntry, startingOffset int) int {
	left, right := 0, len(entries)-1

	for left <= right {
		mid := left + (right-left)/2
		currentOffset := entries[mid].Offset

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
