#!/bin/sh

echo "Building for Centos/Fedora/RHEL"
yum -y install epel-release
yum update -y
yum install -y gcc gcc-c++ make
yum install -y rpm-build
yum install -y boost-devel
yum install -y apr-devel apr-util-devel apr-util-sqlite
yum install -y log4cxx log4cxx-devel
yum install -y jemalloc jemalloc-devel
yum install -y sqlite sqlite-devel
yum install -y libmicrohttpd libmicrohttpd-devel
yum install -y muParser muParser-devel
yum install -y cmake
