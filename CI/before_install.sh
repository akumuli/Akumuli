#!/bin/bash

echo "Running install script for $TRAVIS_OS_NAME"
echo "Work dir: $(pwd)"

if [[ "$TRAVIS_OS_NAME" == "osx" ]];   then brew update;              fi
if [[ "$TRAVIS_OS_NAME" == "osx" ]];   then sh prerequisites-osx.sh;  fi
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then sudo apt-get update;      fi
if [[ "$TRAVIS_OS_NAME" == "linux" ]]; then sudo sh prerequisites.sh; fi
