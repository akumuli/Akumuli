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
    "arch") pkgman='pacman';; #Arch Linux
esac


if [ "x$pkgman" = "xyum" -o "x$pkgman" = "xdnf" ]; then
        echo "Building for Centos/Fedora/RHEL"
        sudo $pkgman install -y cmake \
                          boost boost-devel \
                          log4cxx log4cxx-devel \
                          sqlite sqlite-devel \
                          apr-devel apr-util-devel apr-util-sqlite \
                          libmicrohttpd-devel \
                          jemalloc-devel
else
        if [ "x$pkgman" = "xapt" ]; then
                echo 'The script will install packages using apt-get.' \
                     'It can ask for your sudo password.'

                echo 'Trying to install cmake'
                sudo apt-get install -y cmake

                echo 'Trying to install boost libraries'
                sudo apt-get install libboost-all-dev

                echo 'Trying to install other libraries'
                sudo apt-get install -y libapr1-dev libaprutil1-dev \
                                        liblog4cxx10-dev liblog4cxx10 liblog4cxx-dev \
                                        libaprutil1-dbd-sqlite3 libsqlite3-dev \
                                        libmicrohttpd-dev \
                                        libjemalloc-dev
        else
                if [ "x$pkgman" = "xpacman" ]; then
                        echo 'The script will install packages using pacman.' \
                             'It can ask for your sudo password.'

                        echo 'Trying to install cmake'
                        sudo pacman --noconfirm -S cmake

                        echo 'Trying to install boost libraries'
                        sudo pacman --noconfirm -S boost boost-libs

                        echo 'Trying to install other libraries'
                        sudo pacman --noconfirm -S log4cxx \
                                                 apr apr-util \
                                                 sqlite \
                                                 libmicrohttpd \
                                                 jemalloc
                else
                        echo "ERROR: Unknown package manager: $distri"
                        exit 1
                fi
        fi
fi #package manager check
