package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateUniqueId() string {
	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	if err != nil {
		log.Fatal(err)
	}

	randomString := base64.URLEncoding.EncodeToString(randomBytes)
	timestamp := time.Now().UnixNano()

	return fmt.Sprintf("%d.%s", timestamp, randomString)
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = generateUniqueId()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
