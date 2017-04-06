#!/bin/sh

distri=$(awk '/^DISTRIB_ID=|^ID=/' /etc/*-release | sed 's/.*=//' | sed 's/"//g' | tr '[:upper:]' '[:lower:]' | head -n1)

if [ "x$distri" = "x" ]; then
	distri=$(find /etc/*-release | head -n1 | xargs grep -o '(\w*)' | sed 's/[()]//g' | tr '[:upper:]' '[:lower:]')
fi

case $distri in
	"rhel") pkgman='yum';;
	"debian") pkgman='apt';;
    "ubuntu") pkgman='apt';;
	"santiago") pkgman='yum';; #RHEL6
	"tikanga") pkgman='yum';; #RHEL5
esac


if [ "x$pkgman" = "xyum" ]; then
	echo "Building for Centos/Fedora/RHEL"
	sudo yum install cmake
	sudo yum install boost boost-devel
	sudo yum install log4cxx log4cxx-devel
	sudo yum install sqlite sqlite-devel
	sudo yum install apr-devel apr-util-devel apr-util-sqlite
	sudo yum install libmicrohttpd-devel
	sudo yum install jemalloc-devel
else
	if [ "x$pkgman" = "xapt" ]; then
		echo 'The script will install packages using apt-get.' \
		     'It can ask for your sudo password.'
		     
		echo 'Trying to install boost libraries'
		sudo apt-get install libboost-all-dev

		echo 'Trying to install other libraries'
        sudo apt-get install -y libapr1-dev libaprutil1-dev libaprutil1-dbd-sqlite3
        sudo apt-get install -y liblog4cxx10-dev liblog4cxx10
        sudo apt-get install -y libjemalloc-dev
        sudo apt-get install -y libsqlite3-dev
        sudo apt-get install -y libmicrohttpd-dev

		echo 'Trying to install cmake'
		sudo apt-get install -y cmake
	else
		echo "ERROR: Unknown package manager: $distri"
		exit 1
	fi
fi #package manager check
