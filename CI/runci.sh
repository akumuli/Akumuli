#!/bin/bash

echo "Running docker.sh script for $1"
echo "Work dir: $(pwd)"

sh ./CI/prerequisites-$1.sh

mkdir build
cd build

if [[ $GENERIC_BUILD == true ]]; then
    cmake .. -DCMAKE_BUILD_TYPE=ReleaseGen;
    if [ $? -ne 0 ]; then
        exit 1
    fi
else
    cmake .. -DCMAKE_BUILD_TYPE=Release;
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

make -j4
if [ $? -ne 0 ]; then
    exit 1
fi

# Run tests
bash ../CI/test-$1.sh
if [ $? -ne 0 ]; then
    exit 1
fi

make install
if [ $? -ne 0 ]; then
    exit 1
fi

cpack
if [ $? -ne 0 ]; then
    exit 1
fi
