#!/bin/bash
echo "Running build script for $TRAVIS_OS_NAME"
echo "Work dir: $(pwd)"

mkdir build
cd build
if [[ $GENERIC_BUILD == true ]]; then
    cmake .. -DCMAKE_BUILD_TYPE=ReleaseGen;
    if [ $? -ne 0 ]; then
        echo "cmake failed" >&2
        exit 1
    fi
else
    cmake .. -DCMAKE_BUILD_TYPE=Release;
    if [ $? -ne 0 ]; then
        echo "cmake failed" >&2
        exit 1
    fi
fi
