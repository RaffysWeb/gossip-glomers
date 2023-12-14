## Challenge #4: Grow-Only Counter

[Challenge](https://fly.io/dist-sys/4/)

In this challenge we need to implement an `add` and `read` handler and ensure that we can implement a grow only counter while using the provided sequentially consistent kv store `seq-kv`.

The workload will include network partitions so some nodes might not always be available, to deal with this we'll store the values in the kv store where the key is the ID of the receiving node and the value its current value, additionally we'll create a `getSum` handler that will retrieve the current sum for that node.
Each node will also have a cache of previously read values so if the node is not available it will use the cached value.
