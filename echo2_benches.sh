#!/bin/bash

set -ex

client=fiona
server=tokio
port=$1
files=15000

ulimit -n 50000

cargo bench --bench echo2 -- --$server --ipv4-addr 127.0.0.1 --port "$port" --server --nr-files $files &
server_pid=$!

cargo bench --bench echo2 -- --$client --ipv4-addr 127.0.0.1 --port "$port" --client --nr-files $files &
client_pid=$!

_terminate() {
  echo "caught SIGINT, reaping child processes"
  kill $client_pid $server_pid
  exit 1
}

trap _terminate SIGINT

wait $client_pid
wait $server_pid
