#!/bin/bash

ERROR() {
    echo "ERROR: "$1 1>&2
    exit 1
}

WARNING() {
    echo "WARNING: "$1 1>&2
}

AKUMULID=`which akumulid`

if [ ! -r ~/.akumulid ] ; then
    # First run, initialize configuration using the template
    sed -e "s/__nvolumes__/$nvolumes/g" -e "s/__volume_size__/$volume_size/" /root/akumulid_template > /root/.akumulid
fi

DBDIR=`cat ~/.akumulid | grep 'path=' | awk -F= '{print $2}'`

echo $DBDIR

if [ "$DBDIR"x = "x" ] ; then
    ERROR "DB path is not configured"
elif [ ! -d $DBDIR ] ; then
    WARNING "DB path and/or DB is not exist. creating"
    mkdir $DBDIR
    ${AKUMULID} --create || ERROR "Can't create database"
elif [ ! -r $DBDIR/db.akumuli ] ; then
    WARNING "DB is not exist. creating"
    ${AKUMULID} --create || ERROR "Can't create database"
fi

exec ${AKUMULID}

