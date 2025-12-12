#!/usr/bin/bash

set -eu

./mvnw -Pnative spring-boot:build-image -Dspring-boot.build-image.imageName=ghcr.io/smukherj1/masonry/server:latest