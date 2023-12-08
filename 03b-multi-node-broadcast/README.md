## Challenge #3b: Multi-Node Broadcast

[Challenge](https://fly.io/dist-sys/3b/)

In this challenge, we'll be building on the previous challenge by adding multiple nodes to the mix.
This will increase the complexity since we need to add locks to prevent race conditions and ensure that we get consistent stored messages across all nodes.

We also have to broadcast to the other nodes in the cluster, for that we'll use the provided topology.
