#!/bin/bash

echo "Running docker.sh script for $TRAVIS_OS_NAME"
echo "Work dir: $(pwd)"

apt-get update
sh prerequisites.sh

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release

make -j4

ctest -VV
