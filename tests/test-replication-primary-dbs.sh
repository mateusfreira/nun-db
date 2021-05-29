#!/bin/bash

command=$1
command_to_run=$2
echo $command;

primaryHttpAddress="127.0.0.1:9092"
primaryTcpAddress="127.0.0.1:3017"
secoundary1HttpAddress="127.0.0.1:9093"
secoundary2HttpAddress="127.0.0.1:9094"
user="mateus"
password="$user"
timeoutSpeep=3
replicaSetAddrs="127.0.0.1:3016,127.0.0.1:3017,127.0.0.1:3018"


if [ $command = "build" ] || [ $command = "all" ] || [ $command = "start-1" ] || [ command = "start-primary" ]
then
    cargo build
fi

if [ $command = "snapshot-all" ] || [ $command = "all" ] || [ $command = "kill" ]
then
    echo "snapshot all dbs"
    RUST_BACKTRACE=1 ./target/debug/nun-db --user $NUN_USER  -p $NUN_PWD --host "http://$primaryHttpAddress" exec "use-db \$admin $password; keys" | tr "," "\n" | sort  | grep -v "lost+found" | tail +5 | xargs  -I '{}' nun-db --user $user  -p $password --host "http://$primaryHttpAddress" exec "replicate-snapshot {}"
fi


if [ $command = "kill" ] || [ $command = "all" ]
then
    cat .primary.pid | xargs -I '{}' kill -9 {}
    cat .secoundary.pid | xargs -I '{}' kill -9 {}

fi
if [ $command = "clean" ] || [ $command = "all" ]
then
    echo "Clean!"
    rm .secoundary.pid
    rm .primary.pid
    rm dbs/*
    rm dbs1/*
    rm dbs2/*
fi

if [ $command = "all" ]
then
    echo "Add trap if all"
    trap "kill 0" EXIT
fi

if [ $command = "start-1" ] || [ $command = "start-primary" ] || [ $command = "all" ]
then
    echo "Starting the primary"
    NUN_DBS_DIR=./dbs RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$primaryHttpAddress" --tcp-address "$primaryTcpAddress" --ws-address "127.0.0.1:3058" --replicate-address "$replicaSetAddrs" >primary.log&
    PRIMARY_PID=$!
    echo $PRIMARY_PID >> .primary.pid
    sleep $timeoutSpeep
fi

if [ $command = "cluster-state" ]
then
    clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
    echo $clusterStatePrimary
fi


if [ $command = "start-2" ] || [ $command = "all" ]
then
    echo "Starting secoundary 1"
    NUN_DBS_DIR=./dbs1 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary1HttpAddress" --tcp-address "127.0.0.1:3016" --ws-address "127.0.0.1:3057" --replicate-address "$replicaSetAddrs">secoundary.log&
    SECOUNDARY_PID=$!
    echo $SECOUNDARY_PID >> .secoundary.pid
    sleep $timeoutSpeep
fi


if [ $command = "start-3" ] || [ $command = "all" ]
then
    echo "Starting secoundary 2"
    NUN_DBS_DIR=./dbs2 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary2HttpAddress" --tcp-address "127.0.0.1:3018" --ws-address "127.0.0.1:3059" --replicate-address "$replicaSetAddrs">secoundary.2.log&
    SECOUNDARY_2_PID=$!
    echo $SECOUNDARY_2_PID >> .secoundary.pid
    sleep $timeoutSpeep 

fi

if [ $command = "all" ]
then
    echo "Giving time to election!!!"
    sleep 10
fi

if [ $command = "save-admin" ] || [ $command = "all" ]
then
        RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "auth mateus mateus; use-db \$admin mateus; snapshot;"
        RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$secoundary1HttpAddress" exec "auth mateus mateus; use-db \$admin mateus; snapshot;"
        RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$secoundary2HttpAddress" exec "auth mateus mateus; use-db \$admin mateus; snapshot;"
fi

if [ $command = "print-election-primary" ] || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "cluster-state";
fi

if [ $command = "print-election-2" ] || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$secoundary1HttpAddress" exec "cluster-state";
fi

if [ $command = "print-election-3" ] || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$secoundary2HttpAddress" exec "cluster-state";
fi


if [ $command = "create-vue" ] || [ $command = "create-all" ]   || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "auth mateus mateus; create-db vue vue_pwd; use-db vue vue_pwd; snapshot";
fi

if [ $command = "create-test" ] || [ $command = "create-all" ]  || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "auth mateus mateus; create-db test test-pwd; use-db test test-pwd; snapshot";
fi

if [ $command = "create-blog" ] || [ $command = "create-all" ]   || [ $command = "all" ]
then
	RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "auth mateus mateus; create-db analitcs-blog analitcs-blog-2903uyi9ewrj; use-db analitcs-blog analitcs-blog-2903uyi9ewrj; snapshot;";
fi

exit 0
