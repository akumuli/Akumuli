#!/bin/bash
echo "Running tests for Ubuntu Xenial"
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
akumulid/akumulid --init --disable-wal
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

echo "Running advanced integration tests"
python functests/test_query_language.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_volume_overflow.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_database_overflow.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_restart.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_kill.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_concurrency.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_ingestion_errors.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_join_query.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_filter_query.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_events.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_eval.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi

echo "Set up unconstrained environment"
python functests/akumulid_test_tools.py set_nvolumes 0

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

echo "Running advanced integration tests"
python functests/test_query_language.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_volume_overflow.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_restart.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_kill.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_concurrency.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_join_query.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_filter_query.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_events.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
bash functests/roundtrip.sh
if [ $? -ne 0 ]; then
    echo "Roundtrip test failed" >&2
    exit 1
fi
