#!/bin/bash


./tests/test-fail-invalid-op-log-dbs.sh
status=$?
if [ $status != 0 ]
then
	echo "Fail fail-invalid-op-log" 
	exit 1
fi

./tests/test-fail-primary-dbs.sh

status=$?
if [ $status != 0 ]
then
	echo "Fail fail-primary" 
	exit 2
fi

./tests/test-replication-dbs.sh
status=$?
if [ $status != 0 ]
then
	echo "Fail test-replication-dbs" 
	exit 3
fi

