#!/bin/bash
echo "Running WAL tests for Ubuntu Trusty"
echo "Work dir: $(pwd)"

echo "Set up disk constrained environment"
akumulid/akumulid --init
python functests/akumulid_test_tools.py set_log_path $TRAVIS_BUILD_DIR/akumuli.log

# TODO: remove
echo "Test configuration"
cat ~/.akumulid

echo "Running base integration tests"
python functests/test_data_ingestion.py akumulid/ TCP
if [ $? -ne 0 ]; then
    # TODO: remove
    cat $TRAVIS_BUILD_DIR/akumuli.log
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
python functests/test_group_aggregate.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
python functests/test_search_api.py akumulid/
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
python functests/test_wal_recovery.py akumulid/
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
python functests/test_group_aggregate.py akumulid/
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
python functests/test_wal_recovery.py akumulid/
if [ $? -ne 0 ]; then
    echo "Advanced test failed" >&2
    exit 1
fi
bash functests/roundtrip.sh
if [ $? -ne 0 ]; then
    echo "Roundtrip test failed" >&2
    exit 1
fi
