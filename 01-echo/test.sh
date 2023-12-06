#!/bin/bash

go build -o bin
../utils/maelstrom test -w echo --bin bin --node-count 1 --time-limit 10
