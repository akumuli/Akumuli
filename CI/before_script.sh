#!/bin/bash
echo "Running build script for $TRAVIS_OS_NAME"
echo "Work dir: $(pwd)"

mkdir build
cd build
if [[ $GENERIC_BUILD == true ]]; then
cmake .. -DCMAKE_BUILD_TYPE=ReleaseGen;
else
cmake .. -DCMAKE_BUILD_TYPE=Release;
fi
