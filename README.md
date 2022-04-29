# hashicorp-benchmark
If data already exists under /tmp/raft, need to delete it by issuing:

```
rm -rf /tmp/raft
```

Initialize like so:

```
./raft -id 0 -ip localhost -nodes localhost,localhost,localhost
./raft -id 1 -ip localhost -nodes localhost,localhost,localhost
./raft -id 2 -ip localhost -nodes localhost,localhost,localhost -bootstrap
```

Call it like so:

`grpcurl -plaintext -d '{"timestamp": 13, "payload": "BOOP"}' localhost:50052 coinbase.mockengine.Pong/Ping`
