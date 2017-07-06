#!/bin/bash

# This scripts writes prepared input into the database, reads it back and compares
# two datasets. The input is stored in S3 bucket.

error=

if [ ! -f resp_1day_1000names_10sec_step.gz ]; then
    # Download data from S3
    echo "Downloading resp_1day_1000names_10sec_step.gz ..."
    wget -q https://s3-us-west-2.amazonaws.com/travis-ci-data/resp_1day_1000names_10sec_step.gz
    echo "resp_1day_1000names_10sec_step.gz downloaded"
fi

if [ ! -f opentsdb_1day_1000names_10sec_step.gz ]; then
    # Download data from S3
    echo "Downloading opentsdb_1day_1000names_10sec_step.gz ..."
    wget -q https://s3-us-west-2.amazonaws.com/travis-ci-data/opentsdb_1day_1000names_10sec_step.gz
    echo "opentsdb_1day_1000names_10sec_step.gz downloaded"
fi

if [ ! -f expected-meta-results.resp ]; then
    # Download data from S3
    echo "Downloading expected-meta-results.resp ..."
    wget -q https://s3-us-west-2.amazonaws.com/travis-ci-data/expected-meta-results.resp
    echo "expected-meta-results.resp downloaded"
fi

if [ ! -f expected-join-results.resp.gz ]; then
    # Download data from S3
    echo "Downloading expected-join-results.resp.gz ..."
    wget -q https://s3-us-west-2.amazonaws.com/travis-ci-data/expected-join-results.resp.gz
    echo "expected-join-results.resp.gz downloaded"
fi

echo "Starting akumulid..."
# Re-initialize the database
./akumulid/akumulid --delete
./akumulid/akumulid --create

# Run akumulid server in the background
./akumulid/akumulid&
pid=$!
sleep 5

# Insert downloaded data
echo "Writing data in RESP format"
time cat resp_1day_1000names_10sec_step.gz | gunzip > /dev/tcp/127.0.0.1/8282
echo "Completed"


# read data back
curl -s --url localhost:8181/api/query -d '{ "select": "meta:names" }' > actual-meta-results.resp

if ! cmp expected-meta-results.resp actual-meta-results.resp >/dev/null 2>&1
then
    error="Metadata query error (RESP)"
fi
curl -s --url localhost:8181/api/query -d '{ "join": ["cpu.user","cpu.sys","cpu.real","idle","mem.commit","mem.virt","iops","tcp.packets.in","tcp.packets.out","tcp.ret"], "range": { "from": "20170101T000000.000000", "to": "20170102T000000.000000" }}' | gzip > actual-join-results.resp.gz

# TODO: check the results
if ! diff -q <(zcat expected-join-results.resp.gz) <(zcat actual-join-results.resp.gz)
then
    error="Join query error(RESP), ${error}"
fi

# Stop the database server
echo "Restarting akumulid..."
kill -INT ${pid}
sleep 5

# Re-initialize the database
./akumulid/akumulid --delete
./akumulid/akumulid --create

# Run akumulid server in the background
./akumulid/akumulid&
pid=$!
sleep 5

# Insert OpenTSDB data
echo "Writing data in OpenTSDB format"
time cat opentsdb_1day_1000names_10sec_step.gz | gunzip > /dev/tcp/127.0.0.1/4242
echo "Completed"

# read data back
curl -s --url localhost:8181/api/query -d '{ "select": "meta:names" }' > actual-meta-results.resp

if ! cmp expected-meta-results.resp actual-meta-results.resp >/dev/null 2>&1
then
    error="Metadata query error (OpenTSDB)"
fi
curl -s --url localhost:8181/api/query -d '{ "join": ["cpu.user","cpu.sys","cpu.real","idle","mem.commit","mem.virt","iops","tcp.packets.in","tcp.packets.out","tcp.ret"], "range": { "from": "20170101T000000.000000", "to": "20170102T000000.000000" }}' | gzip > actual-join-results.resp.gz

# TODO: check the results
if ! diff -q <(zcat expected-join-results.resp.gz) <(zcat actual-join-results.resp.gz)
then
    error="Join query error(OpenTSDB), ${error}"
fi

if [ -n "${error}" ]; then
    echo "${error}"
    exit 1
fi
