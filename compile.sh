#!/usr/bin/env bash

clear
cmake --build build
cd build
mv database ..
cd ..
./database
rm database
