#!/bin/bash

# This is the entery point of any CI job (VM or container based).
# Argument $1 is a name of the distribution which will be used to
# locate the auxilary scripts. For instance test-ubuntu-16.04.sh
# will be used if the $1 is equal to 'ubuntu-16.04'. All build
# variants should have their own prerequisistes-<variant>.sh and
# test-<variant>.sh scripts in CI directory.
#
# If the docker container is used, /opt/akumui directory should
# be mapped to the travis work dir. All build artefacts will be
# located there. The 'GENERIC_BUILD' environmental variable is
# used by the script to enable or disable the generic build arch.
# This variable should be passed to docker container using the -e
# option.

echo "Running docker.sh script for $1"
echo "Work dir: $(pwd)"

sh ./CI/prerequisites-$1.sh

mkdir build
cd build

if [[ $GENERIC_BUILD == true ]]; then
    echo "GENERIC BUILD"
    cmake .. -DCMAKE_BUILD_TYPE=ReleaseGen;
    if [ $? -ne 0 ]; then
        exit 1
    fi
else
    echo "SKYLAKE BUILD"
    cmake .. -DCMAKE_BUILD_TYPE=Release;
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

make -j4
if [ $? -ne 0 ]; then
    exit 1
fi

# Run tests
#bash ../CI/test-$1.sh
#if [ $? -ne 0 ]; then
#    exit 1
#fi

# This step is not required by the VM build with ubuntu-14.04,
# but with newer ubuntu 'cpack' call will fail without this step.
# To fix this the 'make install' is performed only if the script
# is executed by root user. The script is started as root in
# container enviroment and as normal user in VM.
if [[ $(id -u) -ne 0 ]]; then
    echo "Running as a normal user, skipping 'make install' step"
else
    echo "Running as root, performing 'make install' step"
    make install
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi

if [[ "$OSTYPE" == "linux-gnu" ]]; then
    GEN=`if [[ -f /etc/debian_version ]]; then echo "DEB"; else echo "RPM"; fi`
    echo "Running CPack -G $GEN"
    cpack -G DEB
    if [ $? -ne 0 ]; then
        exit 1
    fi
fi
