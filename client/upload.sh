#!/bin/bash

set -eu

# Build the client binary
GOOS=linux GOARCH=amd64 go build -o out/upload ./bin/upload.go

# Run the client binary
./out/upload --upload-file=data/transactions.json --upload-id=1