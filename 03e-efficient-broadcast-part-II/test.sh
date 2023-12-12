#!/bin/bash

go build -o bin
../utils/maelstrom test -w broadcast --bin bin --node-count 25 --time-limit 20 --rate 100 --latency 100
