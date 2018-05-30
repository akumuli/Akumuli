#!/bin/sh

distri=$(awk '/^DISTRIB_ID=|^ID=/' /etc/*-release | sed 's/.*=//' | sed 's/"//g' | tr '[:upper:]' '[:lower:]' | head -n1)

if [ "x$distri" = "x" ]; then
	distri=$(find /etc/*-release | head -n1 | xargs grep -o '(\w*)' | sed 's/[()]//g' | tr '[:upper:]' '[:lower:]')
fi

case $distri in
    "rhel") pkgman='yum';;
    "centos") pkgman='yum';;
    "fedora") pkgman='dnf';;
    "debian") pkgman='apt';;
    "ubuntu") pkgman='apt';;
    "santiago") pkgman='yum';; #RHEL6
    "tikanga") pkgman='yum';; #RHEL5
esac


if [ "x$pkgman" = "xyum" -o "x$pkgman" = "xdnf" ]; then
	echo "Building for Centos/Fedora/RHEL"
        $pkgman install -y cmake \
                          boost boost-devel \
                          log4cxx log4cxx-devel \
                          sqlite sqlite-devel \
                          apr-devel apr-util-devel apr-util-sqlite \
                          libmicrohttpd-devel \
                          jemalloc-devel
else
	if [ "x$pkgman" = "xapt" ]; then
                echo 'The script will install packages using apt-get.'
		     
		echo 'Trying to install boost libraries'
                apt-get install -y libboost-all-dev

		echo 'Trying to install other libraries'
        apt-get install -y libapr1-dev libaprutil1-dev libaprutil1-dbd-sqlite3
        apt-get install -y liblog4cxx10-dev liblog4cxx10
        apt-get install -y libjemalloc-dev
        apt-get install -y libsqlite3-dev
        apt-get install -y libmicrohttpd-dev

		echo 'Trying to install cmake'
                apt-get install -y cmake
	else
		echo "ERROR: Unknown package manager: $distri"
		exit 1
	fi
fi #package manager check
