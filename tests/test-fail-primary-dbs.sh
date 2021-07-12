#!/bin/bash

command=$1
echo $command;

primaryHttpAddress="127.0.0.1:9092"
primaryTcpAddress="127.0.0.1:3017"
secoundary1HttpAddress="127.0.0.1:9093"
secoundary2HttpAddress="127.0.0.1:9094"
user="mateus"
password="$user"
timeoutSpeep=3
replicaSetAddrs="127.0.0.1:3017,127.0.0.1:3016,127.0.0.1:3018"

cargo build

if [ $command = "all" ]
then
    echo "Add trap if all"
    trap "kill 0" EXIT
    echo "Will clean up the dbs"
     ./tests/commons.sh kill
     ./tests/commons.sh clean 
     sleep $timeoutSpeep
fi

if [ $command = "start-1" ] || [ $command = "all" ]
then
echo "Starting the primary"
NUN_DBS_DIR=/tmp/dbs RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$primaryHttpAddress" --tcp-address "$primaryTcpAddress" --ws-address "127.0.0.1:3058" --replicate-address "$replicaSetAddrs" >primary.log&
PRIMARY_PID=$!
echo $PRIMARY_PID >> .primary.pid
sleep $timeoutSpeep
fi

if [ $command = "start-2" ] || [ $command = "all" ]
then
echo "Starting secoundary 1"
NUN_DBS_DIR=/tmp/dbs1 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary1HttpAddress" --tcp-address "127.0.0.1:3016" --ws-address "127.0.0.1:3057" --replicate-address "$replicaSetAddrs" >secoundary.log&
SECOUNDARY_PID=$!
echo $SECOUNDARY_PID >> .secoundary.pid
sleep $timeoutSpeep
fi


if [ $command = "start-3" ] || [ $command = "all" ]
then
echo "Starting secoundary 2"
NUN_DBS_DIR=/tmp/dbs2 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary2HttpAddress" --tcp-address "127.0.0.1:3018" --ws-address "127.0.0.1:3059" --replicate-address "$replicaSetAddrs" >secoundary.2.log&
SECOUNDARY_2_PID=$!
echo $SECOUNDARY_2_PID >> .secoundary.pid
sleep $timeoutSpeep
fi

if [ $command = "all" ]
then
    echo "Giving time to election!!!"
    sleep 3
fi

if [ $command = "cluster-state" ]
then
    clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
    echo $clusterStatePrimary
fi


if [ $command = "election" ] || [ $command = "all" ]
then
clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
clusterStateSecoundary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth $user $user; cluster-state;")
clusterStateSecoundary2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "auth $user $user; cluster-state;")

if [ "$clusterStatePrimary" != "$clusterStateSecoundary" ]; then
    echo "Cluster state should be the same in all the cluster members! "
    echo "Primary: $clusterStatePrimary \n Secoundary: $clusterStateSecoundary"
    exit 1
fi

if [ "$clusterStatePrimary" != "$clusterStateSecoundary2" ]; then
    echo "Cluster state should be the same in all the cluster members! "
    echo "Primary: $clusterStatePrimary \n Secoundary2: $clusterStateSecoundary2"
    exit 1
fi

echo "Change cluster state primary : $clusterStatePrimary"
echo "Change cluster state secoundary : $clusterStateSecoundary"
fi # if [ $command = "election" ] || [ $command = "all" ]    

if [ $command = "all" ]
then
    curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; create-db test-db test-db-key;"
    echo "Will test write in the primary read from secoundaries"
    sleep $timeoutSpeep
    start_time="$(date -u +%s)"

    for i in {1..20}
    do
        echo "Set in the primary"
        r=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; set state jose-$i-1;")
        echo "Read from the secoundary"
        get_result=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "use-db test-db test-db-key; get state")
        get_result2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")
        if [ "$get_result" != "empty;value jose-$i-1" ]; then
            echo "Invalid value value in the secoundary 1: $get_result $i"
            exit 2
        else
            if [ "$get_result2" != "empty;value jose-$i-1" ]; then
                echo "Invalid value value in the secoundary 2: $get_result $i"
                exit 3
            else
                echo "Request $i Ok"
            fi 
        fi

        echo "Delete testing"

        deleteResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; remove state jose-$i-1;")
        echo "Read from the secoundary"
        get_result=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "use-db test-db test-db-key; get state")
        get_result2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")
        if [ "$get_result" != "empty;value <Empty>" ]; then
            echo "Invalid value value in the secoundary 1: $get_result $i"
            exit 2
        else
            if [ "$get_result2" != "empty;value <Empty>" ]; then
                echo "Invalid value value in the secoundary 2: $get_result $i"
                exit 3
            else
                echo "Request $i Ok"
            fi 
        fi
    done
    end_time="$(date -u +%s)"
    elapsed="$(($end_time-$start_time))"
    echo "Total of $elapsed seconds elapsed for process"

    echo  "Will snapshot all admin dbs before killing them and the test database"
    snapshotResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; use-db test-db test-db-key; snapshot")
    echo "Snapshot result $snapshotResult"
    ./tests/commons.sh save-admin
    sleep $timeoutSpeep

    echo "Will start the tests of failure"

    kill -9 $PRIMARY_PID
    echo 'Rebuilding the cluster'
    sleep 10
    sleep $timeoutSpeep
    clusterStatePrimary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth $user $user; cluster-state;")
    echo $clusterStatePrimary


    r=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; set state $user;")
    sleep $timeoutSpeep
    get_result=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "use-db test-db test-db-key; get state")

    echo "Check the log..."
    sleep $timeoutSpeep

    echo  "Result: $get_result"

    if [ "$get_result" != "empty;value $user" ]; then
        echo "Invalid value value in the secoundary 1."
        exit 2
    fi


    for i in {1..20}
    do
        echo "Set in the primary"
        r=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; set state jose-$i-1;")
        echo "Read from the secoundary"
        get_result2=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "use-db test-db test-db-key; get state")
        if [ "$get_result2" != "empty;value jose-$i-1" ]; then
            echo "Invalid value value in the secoundary 2: $get_result $i"
            exit 3
        else
            echo "Request $i Ok"
        fi
    done

    clusterStatePrimary=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "auth $user $user; cluster-state;")
    expectedCluster="valid auth
;cluster-state  127.0.0.1:3016:Primary, 127.0.0.1:3018:Secoundary,"
    echo "Cluster state $clusterStatePrimary - $expectedCluster"

    if [ "$clusterStatePrimary" !=  "$expectedCluster" ]; then
        echo "Invalid cluster state after kill"
        exit 4
    else
        echo "Request Ok"
    fi
    echo "Will restart primary"
     ./tests/commons.sh start-1
    echo "Will wait for the replication"
     sleep 10 # 
     echo "Read from the secoundary"
     get_result2=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; get state")
     if [ "$get_result2" != "empty;value jose-20-1" ]; then
        echo "Invalid value value in the secoundary 2: $get_result"
        exit 3
     else
        echo "Request restored from op log Ok"
      fi
fi

exit 0

