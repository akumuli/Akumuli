#!/bin/bash
echo "Running tests for $TRAVIS_OS_NAME"
echo "Work dir: $(pwd)"

echo "Running the build"
make -j4

echo "Running unit-tests"
if [[ $UNIT_TEST == true ]]; then ctest -VV; fi
if [[ $FUNC_TEST_BASE == true ]]; then functests/storage_test /tmp; fi

echo "Set up disk constrained environment"
akumulid/akumulid --init
python functests/akumulid_test_tools.py set_log_path $HOME/akumuli.log

if [[ $FUNC_TEST_BASE == true ]]; then
    echo "Running base integration tests"
    python functests/test_data_ingestion.py akumulid/ TCP
    python functests/test_data_ingestion.py akumulid/ UDP
    python functests/test_data_ingestion_bulk.py akumulid/ TCP
    python functests/test_data_ingestion_bulk.py akumulid/ UDP
fi

if [[ $FUNC_TEST_ADVANCED == true ]]; then
    echo "Running advanced integration tests"
    python functests/test_query_language.py akumulid/
    python functests/test_volume_overflow.py akumulid/
    python functests/test_database_overflow.py akumulid/
    python functests/test_restart.py akumulid/
    python functests/test_kill.py akumulid/
    python functests/test_concurrency.py akumulid/
    python functests/test_ingestion_errors.py akumulid/
    python functests/test_join_query.py akumulid/
    python functests/test_filter_query.py akumulid/
fi

echo "Set up unconstrained environment"
python functests/akumulid_test_tools.py set_nvolumes 0

if [[ $FUNC_TEST_BASE == true ]]; then
    echo "Running base integration tests"
    python functests/test_data_ingestion.py akumulid/ TCP
    python functests/test_data_ingestion.py akumulid/ UDP
    python functests/test_data_ingestion_bulk.py akumulid/ TCP
    python functests/test_data_ingestion_bulk.py akumulid/ UDP
fi

if [[ $FUNC_TEST_ADVANCED == true ]]; then
    echo "Running advanced integration tests"
    python functests/test_query_language.py akumulid/
    python functests/test_volume_overflow.py akumulid/
    python functests/test_restart.py akumulid/
    python functests/test_kill.py akumulid/
    python functests/test_concurrency.py akumulid/
    python functests/test_join_query.py akumulid/
    python functests/test_filter_query.py akumulid/
    bash functests/roundtrip.sh
fi

# Build deb package and docker image only on Linux
if [[ $DEPLOY_IMAGE == true ]]; then
    cpack;
    cp akumuli_*_amd64.deb ./docker;
    export VERSION=`ls akumuli_*_amd64.deb | sed -n 's/akumuli_\([0-9].[0-9].[0-9][0-9]\)-1ubuntu1.0_amd64\.deb/\1/p'`
    export REPO=`if [[ $TRAVIS_PULL_REQUEST == "false" ]]; then echo "akumuli/akumuli"; else echo "akumuli/test"; fi`;
    export TAG=`if [[ $GENERIC_BUILD == "false" ]]; then echo "skylake"; else echo "generic"; fi`;
    docker build -t $REPO:$VERSION-$TAG ./docker;
fi
