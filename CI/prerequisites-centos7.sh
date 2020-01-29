#!/bin/sh

echo "Building for Centos/Fedora/RHEL"
yum install -y epel-release
yum update  -y
yum install -y centos-release-scl
yum install -y devtoolset-7
source scl_source enable devtoolset-7
yum install -y rpm-build
yum install -y boost-devel
yum install -y apr-devel apr-util-devel apr-util-sqlite
yum install -y log4cxx log4cxx-devel
yum install -y jemalloc jemalloc-devel
yum install -y sqlite sqlite-devel
yum install -y libmicrohttpd libmicrohttpd-devel
yum install -y cmake
