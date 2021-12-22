#!/bin/bash

cargo build --release --locked
mkdir -p ./dist

case "$OSTYPE" in
    darwin*)
        EXECUTABLE=darwin-amd64
    ;;
    *)
        EXECUTABLE=linux-amd64
    ;;
esac

mv target/release/gh-sql "./dist/$EXECUTABLE"
