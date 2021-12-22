#!/bin/bash

cargo build --release --locked
mkdir -p ./dist
mv target/release/gh-sql ./dist/linux-amd64
