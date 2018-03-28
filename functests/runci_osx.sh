# This script doesn't contain all the test that `runci_linux.sh` has.
# This is due to poor OSX build infrastructure on Travis-CI. The VM
# is too slow to run all tests.
functests/storage_test /tmp
akumulid/akumulid --init
python functests/akumulid_test_tools.py set_log_path $HOME/akumuli.log
python functests/test_data_ingestion.py akumulid/ TCP
python functests/test_data_ingestion.py akumulid/ UDP
python functests/test_data_ingestion_bulk.py akumulid/ TCP
python functests/test_data_ingestion_bulk.py akumulid/ UDP
python functests/test_query_language.py akumulid/
python functests/test_volume_overflow.py akumulid/
python functests/test_database_overflow.py akumulid/
python functests/test_restart.py akumulid/
python functests/test_kill.py akumulid/
python functests/test_ingestion_errors.py akumulid/
python functests/test_join_query.py akumulid/
