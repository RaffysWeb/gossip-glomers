#!/bin/bash

go build -o bin
../utils/maelstrom test -w broadcast --bin bin --node-count 1 --time-limit 20 --rate 10
