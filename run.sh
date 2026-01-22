#!/usr/bin/env bash
set -e

echo "Building..."
g++ -std=c++20 -Wall -Wextra -O2 main.cpp indexer.cpp -o test
echo "Running..."

./test