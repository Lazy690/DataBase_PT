#!/usr/bin/env bash
set -e

echo "Building..."
g++ -std=c++20 -Wall -Wextra -O2 new_execution.cpp indexer.cpp -o test
echo "Running..."

./test

