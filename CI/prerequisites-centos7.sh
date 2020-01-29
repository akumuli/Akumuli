#!/bin/sh

echo "Building for Centos/Fedora/RHEL"
yum install -y centos-release-scl
yum install -y devtoolset-8
yum install -y devtoolset-8-toolchain
scl enable devtoolset-8 bash
yum install -y epel-release
yum update  -y
yum install -y rpm-build
yum install -y boost-devel
yum install -y apr-devel apr-util-devel apr-util-sqlite
yum install -y log4cxx log4cxx-devel
yum install -y jemalloc jemalloc-devel
yum install -y sqlite sqlite-devel
yum install -y libmicrohttpd libmicrohttpd-devel
yum install -y cmake
