#!/bin/bash

echo "Running docker.sh script for $1"
echo "Work dir: $(pwd)"

apt-get update
sh ./CI/prerequisites-$1.sh

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
if [ $? -ne 0 ]; then
    exit 1
fi

make -j4
if [ $? -ne 0 ]; then
    exit 1
fi

ctest -VV
if [ $? -ne 0 ]; then
    exit 1
fi

cpack
if [ $? -ne 0 ]; then
    exit 1
fi
