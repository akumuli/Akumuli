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

# Some encoded text data can still be in the receive buffer so we need to wait
# until everyting will be written to disk.
response=
c=0
printf "Waiting until write will be completed "
until [ -n "$response" ]; do
    response=$(curl -s --url localhost:8181/api/query -d '{ "select": "cpu.user", "range": { "from": "20170101T235959.000000", "to": "20170103T000000.000000" }}')
    printf '.'
    sleep 1
    ((c++)) && ((c==20)) && break
done
printf "\nCompleted\n"


# read data back
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Query metadata    -- $timestamp"
curl -s --url localhost:8181/api/query -d '{ "select": "meta:names" }' > actual-meta-results.resp
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Done              -- $timestamp"

diffres=$(diff expected-meta-results.resp actual-meta-results.resp | head -20)
if [ -n "$diffres" ]; then
    echo "Metadata query error! (RESP)"
    echo "Output truncated to 20 lines"
    printf "\n$diffres\n"
    error="Metadata query error (RESP)"
fi

timestamp=$(date +%Y%m%dT%H%M%S)
echo "Query data points -- $timestamp"
curl -s --url localhost:8181/api/query -d '{ "join": ["cpu.user","cpu.sys","cpu.real","idle","mem.commit","mem.virt","iops","tcp.packets.in","tcp.packets.out","tcp.ret"], "range": { "from": "20170101T000000.000000", "to": "20170102T000010.000000" }}' | gzip > actual-join-results.resp.gz
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Done              -- $timestamp"

diffres=$(diff <(zcat resp_1day_1000names_10sec_step.gz) <(zcat actual-join-results.resp.gz) | head -100)
# check the results
if [ -n "$diffres" ];
then
    echo "Join query error!(RESP)"
    echo "Output truncated to 100 lines"
    printf "\n$diffres\n"
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

# Some encoded text data can still be in the receive buffer so we need to wait
# until everyting will be written to disk.
response=
c=0
printf "Waiting until write will be completed "
until [ -n "$response" ]; do
    response=$(curl -s --url localhost:8181/api/query -d '{ "select": "cpu.user", "range": { "from": "20170101T235959.000000", "to": "20170103T000000.000000" }}')
    printf '.'
    sleep 1
    ((c++)) && ((c==20)) && break
done
printf "\nCompleted\n"

# read data back
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Query metadata    -- $timestamp"
curl -s --url localhost:8181/api/query -d '{ "select": "meta:names" }' > actual-meta-results-opentsdb.resp
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Done              -- $timestamp"

diffres=$(diff expected-meta-results.resp actual-meta-results-opentsdb.resp | head -20)
if [ -n "$diffres" ];
then
    echo "Metadata query error! (OpenTSDB)"
    echo "Output truncated to 20 lines"
    printf "\n$diffres\n"
    error="Metadata query error (OpenTSDB), ${error}"
fi

timestamp=$(date +%Y%m%dT%H%M%S)
echo "Query data points -- $timestamp"
curl -s --url localhost:8181/api/query -d '{ "join": ["cpu.user","cpu.sys","cpu.real","idle","mem.commit","mem.virt","iops","tcp.packets.in","tcp.packets.out","tcp.ret"], "range": { "from": "20170101T000000.000000", "to": "20170102T000010.000000" }}' | gzip > actual-join-results-opentsdb.resp.gz
timestamp=$(date +%Y%m%dT%H%M%S)
echo "Done              -- $timestamp"

# check the results
diffres=$(diff <(zcat resp_1day_1000names_10sec_step.gz) <(zcat actual-join-results-opentsdb.resp.gz) | head -100)
if [ -n "$diffres" ];
then
    echo "Join query error!(OpenTSDB)"
    echo "Output truncated to 100 lines"
    printf "\n$diffres\n"
    error="Join query error(OpenTSDB), ${error}"
fi

echo "Stopping akumulid..."
kill -INT ${pid}

if [ -n "${error}" ]; then
    echo "Test failed!"
    echo "${error}"
    exit 1
fi
