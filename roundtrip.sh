#!/bin/bash

# This scripts writes prepared input into the database, reads it back and compares
# two datasets. The input is stored in S3 bucket.

# Re-initialize the database
./akumulid --delete
./akumulid --CI

# Run akumulid server in the background
./akumulid&
pid=$!

# Download data from S3
wget https://s3-us-west-2.amazonaws.com/travis-ci-data/resp_1day_1000names_10sec_step.gz

# Insert downloaded data
time cat resp_1day_1000names_10sec_step.gz | gunzip > /dev/tcp/127.0.0.1/8282

# TODO: read data back

# TODO: compare

# Stop the database server
kill -INT ${pid}
