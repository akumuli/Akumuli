#!/bin/sh

echo 'The script will install packages using apt-get.' \
     'It will ask for your sudo password.'
sudo apt-get install libboost-dev libboost-system-dev libboost-thread-dev \
     libboost-filesystem-dev libboost-test-dev libboost-coroutine-dev \
     libboost-context-dev
sudo apt-get install libgoogle-perftools-dev
sudo apt-get install cmake
