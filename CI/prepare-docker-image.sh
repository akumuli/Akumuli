#!/bin/bash

# This script should be sourced

cd build

echo "Prepare docker image"
echo "Work dir: $(pwd)"

# Build deb package and docker image only on Linux
cpack;
if [ $? -ne 0 ]; then
    echo "cpack failed" >&2
    exit 1
fi
cp akumuli_*_amd64.deb ./docker;
export VERSION=`ls akumuli_*_amd64.deb | sed -n 's/akumuli_\([0-9].[0-9].[0-9][0-9]\)-1ubuntu1.0_amd64\.deb/\1/p'`
if [[ $TRAVIS_PULL_REQUEST == "false" && $TRAVIS_BRANCH == "master" ]];  then
    # master branch build goes to main repo
    export REPO="akumuli/akumuli"
else
    # pull requests and other builds goes to test
    export REPO="akumuli/test"
fi
export TAG=`if [[ $GENERIC_BUILD == "false" ]]; then echo "skylake"; else echo "generic"; fi`;
docker build -t $REPO:$VERSION-$TAG ./docker;
if [ $? -ne 0 ]; then
    echo "docker build failed" >&2
    exit 1
fi

echo "Version: $VERSION"
echo "Docker repository: $REPO"
echo "Docker tag: $TAG"
