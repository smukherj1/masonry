#!/bin/bash

mkdir -p generated

protoc --proto_path=proto --go_out=./generated --go_opt=paths=source_relative \
    --go_opt=Mdigest.proto=github.com/smukherj1/masonry/client/generated \
    --go_opt=Mblobservice.proto=github.com/smukherj1/masonry/client/generated \
    --go-grpc_out=./generated --go-grpc_opt=paths=source_relative \
    --go-grpc_opt=Mdigest.proto=github.com/smukherj1/masonry/client/generated \
    --go-grpc_opt=Mblobservice.proto=github.com/smukherj1/masonry/client/generated \
    proto/*.proto