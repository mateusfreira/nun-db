#!/bin/sh
./tests/commons.sh kill 2> /dev/null
./tests/commons.sh clean 2> /dev/null
./tests/commons.sh start-1 2> /dev/null
./tests/commons.sh create-sample 2> /dev/null
 time for j in {1..100}; do ./target/debug/nun-db -p mateus -u mateus --host "http://127.0.0.1:9092" exec "use-db sample sample-pwd;keys;set jose$j $j;get jose;remove jose; get jose; snapshot "; done

echo "========================================This is great success \o/!!!!!======================="

echo "Will clean up the dbs"
./tests/commons.sh kill 2>/dev/null
./tests/commons.sh clean 
