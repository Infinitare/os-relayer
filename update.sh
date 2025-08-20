#!/bin/bash

git pull
cargo build --release
sudo systemctl stop os-relayer
sudo cp target/release/os-relayer /etc/relayer/
sudo systemctl start os-relayer
