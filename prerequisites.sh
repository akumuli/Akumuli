#!/bin/sh

echo 'The script will install packages using apt-get.' \
     'It can ask for your sudo password.'
     
echo 'Trying to install boost libraries'
sudo apt-get install -y libboost-dev libboost-system-dev libboost-thread-dev \
     libboost-filesystem-dev libboost-test-dev 
sudo apt-get install -y libboost-coroutine-dev \
     libboost-context-dev \
     libboost-program-options-dev libboost-regex-dev
     
echo 'Trying to install other libraries'
sudo apt-get install -y libapr1-dev libaprutil1-dev libaprutil1-dbd-sqlite3
sudo apt-get install -y liblog4cxx10-dev liblog4cxx10
sudo apt-get install -y libjemalloc-dev
sudo apt-get install -y libsqlite3-dev

echo 'Trying to install cmake'
sudo apt-get install -y cmake
