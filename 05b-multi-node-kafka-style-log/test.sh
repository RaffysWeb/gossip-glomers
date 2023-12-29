#!/bin/bash

go build -o bin
../utils/maelstrom test -w kafka --bin bin --node-count 1 --concurrency 2n --time-limit 5 --rate 1
# ../utils/maelstrom test -w kafka --bin bin --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
