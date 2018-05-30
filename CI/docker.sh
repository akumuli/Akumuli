#!/bin/bash

echo "Running docker.sh script for $TRAVIS_OS_NAME" > docker.out
echo "Work dir: $(pwd)" >> docker.out

sudo apt-get update >> docker.out 2>&1
sudo sh prerequisites.sh >> docker.out 2>&1

mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release >> docker.out 2>&1

make -j4 >> docker.out 2>&1

ctest -VV >> docker.out 2>&1
