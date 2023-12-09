## Challenge #3c: Multi-Node Broadcast with network partitions

[Challenge](https://fly.io/dist-sys/3c/)

In this challenge, we'll be building on the previous challenge by making it fault tolerant.
For that we're are using the SyncRPC API that will return errors if the request fails and we'll retry the request until it succeeds.
