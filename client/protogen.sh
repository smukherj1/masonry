#!/bin/bash

mkdir -p generated

# Array of proto files
PROTO_FILES=(
    "digest.proto"
    "blobservice.proto"
    "blobuploadservice.proto"
)

# Common package path
PACKAGE="github.com/smukherj1/masonry/client/generated"

# Base flags
CMD_FLAGS=(
    "--proto_path=proto"
    "--go_out=./generated"
    "--go_opt=paths=source_relative"
    "--go-grpc_out=./generated"
    "--go-grpc_opt=paths=source_relative"
)

# Generate dynamic flags from the array
for proto in "${PROTO_FILES[@]}"; do
    CMD_FLAGS+=("--go_opt=M${proto}=${PACKAGE}")
    CMD_FLAGS+=("--go-grpc_opt=M${proto}=${PACKAGE}")
done

# execute protoc with the generated flags and input files
protoc "${CMD_FLAGS[@]}" proto/*.proto