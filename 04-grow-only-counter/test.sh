#!/bin/bash

go build -o bin
../utils/maelstrom test -w g-counter --bin bin --node-count 3 --rate 100 --time-limit 20 --nemesis partition
