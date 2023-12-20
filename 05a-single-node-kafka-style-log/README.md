## Challenge #5a: Single-Node Kafka-Style Log

[Challenge](https://fly.io/dist-sys/5a/)

In this challenge we want to implement a single node Kafka-like logging service.
This one is pretty straightforward, we just have to implement the handlers for the following endpoints: `send`, `poll`, `commit_offsets` and `list_committed_offsets`.

The poll handler will receive an offset and return all the messages from that offset onwards.
