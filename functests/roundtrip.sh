#!/bin/bash

# This scripts writes prepared input into the database, reads it back and compares
# two datasets. The input is stored in S3 bucket.

# Re-initialize the database
./akumulid/akumulid --delete
./akumulid/akumulid --create

# Run akumulid server in the background
./akumulid/akumulid&
pid=$!

# Download data from S3
echo "downloading resp_1day_1000names_10sec_step.gz ..."
wget -q https://s3-us-west-2.amazonaws.com/travis-ci-data/resp_1day_1000names_10sec_step.gz
echo "resp_1day_1000names_10sec_step.gz downloaded"

# Insert downloaded data
echo "writing data"
time cat resp_1day_1000names_10sec_step.gz | gunzip > /dev/tcp/127.0.0.1/8282
echo "completed, removing resp_1day_1000names_10sec_step.gz ..."
rm resp_1day_1000names_10sec_step.gz


# read data back
curl -s --url localhost:8181/api/query -d '{ "select": "meta:names" }' > actual-meta-results.resp
curl -s --url localhost:8181/api/query -d '{ "join": ["cpu.user","cpu.sys","cpu.real","idle","mem.commit","mem.virt","iops","tcp.packets.in","tcp.packets.out","tcp.ret"], "range": { "from": "20170101T000000.000000", "to": "20170102T000000.000000" }}' > actual-join-results.resp

# TODO: check the results

# Stop the database server
kill -INT ${pid}
