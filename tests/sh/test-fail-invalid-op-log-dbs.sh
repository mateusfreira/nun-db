#!/bin/bash

echo "Add trap if all"
trap "kill 0" EXIT

./tests/commons.sh clean
echo "Start primary" 
./tests/commons.sh start-1
echo "Start secoundary" 
./tests/commons.sh start-2
echo "Send values" 
nc localhost 3017 < tests/add-values-no-snapshot.in

size=$(ls -lh /tmp/dbs/oplog-nun.op | awk '{ print $5 }')
if [ $size == "0B" ]
then
	echo "Invalid oplog size. it should have records!";
	exit 2
fi

echo "Kill primary" 
cat .primary.pid | xargs -I '{}' kill -9 {}
cat .secoundary.pid | xargs -I '{}' kill -9 {}

echo "Start primary" 
./tests/commons.sh start-1
size=$(ls -lh /tmp/dbs/oplog-nun.op | awk '{ print $5 }')
if [ $size != "0B" ]
then
	echo "Invalid op size in the end";
	exit 3
fi
./tests/commons.sh start-2
./tests/commons.sh kill
./tests/commons.sh clean

echo "Great success \o/" 

