#!/bin/bash
echo "Running tests for centos7"
echo "Work dir: $(pwd)"

echo "Running unit-tests"
ctest -VV
if [ $? -ne 0 ]; then
    # We have to manually check the exit code since
    # Travis-CI doesn't work well with `set -e`.
    echo "Unit-tests failed" >&2
    exit 1
fi

functests/storage_test /tmp;
if [ $? -ne 0 ]; then
    echo "API test failed" >&2
    exit 1
fi

echo "Set up disk constrained environment"
akumulid/akumulid --init
python functests/akumulid_test_tools.py set_log_path /opt/akumuli/akumuli.log

echo "Running base integration tests"
python functests/test_data_ingestion.py akumulid/ TCP
if [ $? -ne 0 ]; then
    echo "Base test failed" >&2
    exit 1
fi
python functests/test_data_ingestion.py akumulid/ UDP
if [ $? -ne 0 ]; then
    echo "Base test failed" >&2
    exit 1
fi
python functests/test_data_ingestion_bulk.py akumulid/ TCP
if [ $? -ne 0 ]; then
    echo "Base test failed" >&2
    exit 1
fi
python functests/test_data_ingestion_bulk.py akumulid/ UDP
if [ $? -ne 0 ]; then
    echo "Base test failed" >&2
    exit 1
fi

bash functests/roundtrip.sh
if [ $? -ne 0 ]; then
    echo "Roundtrip test failed" >&2
    exit 1
fi
