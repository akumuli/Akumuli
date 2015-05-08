#!/bin/sh

echo 'The script will install packages using apt-get.' \
     'It will ask for your sudo password.'
     
echo 'Trying to install boost libraries'
sudo apt-get install libboost-dev libboost-system-dev libboost-thread-dev \
     libboost-filesystem-dev libboost-test-dev 
sudo apt-get install libboost-coroutine-dev \
     libboost-context-dev
     
echo 'Trying to install other libraries'
sudo apt-get install libapr1-dev libaprutil1-dev libmicrohttpd-dev

echo 'Trying to install cmake'
sudo apt-get install cmake
