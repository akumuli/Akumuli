#!/bin/sh

echo "Building for Centos/Fedora/RHEL"
yum update
yum install -y boost-devel
yum install -y apr-devel apr-util-devel apr-util-sqlite
yum install -y log4cxx log4cxx-devel
yum install -y jemalloc-devel
yum install -y sqlite sqlite-devel
yum install -y libmicrohttpd-devel
yum install -y cmake
