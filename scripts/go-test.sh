#!/bin/bash
rm -rf /temp/test-raft*

tmux kill-pane -a -t 0 && GO111MODULE=on go test -v --count=1 --parallel=1 -p=1 $1 -run $2 | nl |tee log.log | ./scripts/stream-log-dispatcher/index.js
