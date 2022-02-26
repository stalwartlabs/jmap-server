#!/bin/bash

#actix_web=info,
if test $1 -ge 2; then
    RUST_BACKTRACE=1 RUST_LOG="jmap_server=debug" cargo run -- --db-path /tmp/server$1 --http-port=`expr $1 + 8080` --gossip-port=`expr $1 + 7910` --seed-nodes 127.0.0.1:7911 --cluster secret
else
    RUST_BACKTRACE=1 RUST_LOG="jmap_server=debug" cargo run -- --db-path /tmp/server1 --http-port=8081 --gossip-port=7911 --cluster secret
fi
