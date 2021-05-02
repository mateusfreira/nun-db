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


cargo build


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


if [ $command = "start-1" ] || [ $command = "all" ]
then
    echo "Starting the primary"
    NUN_DBS_DIR=./dbs RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$primaryHttpAddress" --tcp-address "$primaryTcpAddress" --ws-address "127.0.0.1:3058" --replicate-address "127.0.0.1:3016,127.0.0.1:3018" >primary.log&
    PRIMARY_PID=$!
    echo $PRIMARY_PID >> .primary.pid
    sleep $timeoutSpeep
fi

if [ $command = "start-2" ] || [ $command = "all" ]
then
    echo "Starting secoundary 1"
    NUN_DBS_DIR=./dbs1 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary1HttpAddress" --tcp-address "127.0.0.1:3016" --ws-address "127.0.0.1:3057" --replicate-address "127.0.0.1:3017,127.0.0.1:3018">secoundary.log&
    SECOUNDARY_PID=$!
    echo $SECOUNDARY_PID >> .secoundary.pid
    sleep $timeoutSpeep
fi


if [ $command = "start-3" ] || [ $command = "all" ]
then
    echo "Starting secoundary 2"
    NUN_DBS_DIR=./dbs2 RUST_BACKTRACE=1 ./target/debug/nun-db --user $user -p $user start --http-address "$secoundary2HttpAddress" --tcp-address "127.0.0.1:3018" --ws-address "127.0.0.1:3059" --replicate-address "127.0.0.1:3017,127.0.0.1:3016">secoundary.2.log&
    SECOUNDARY_2_PID=$!
    echo $SECOUNDARY_2_PID >> .secoundary.pid
    sleep $timeoutSpeep
fi


if [ $command = "all" ]
then
    echo "Giving time to election!!!"
    sleep 10
fi
if [ $command = "election" ] || [ $command = "all" ]
then
    echo "Will Connect the secoundaries to the primary"
    echo "Election result: $electionResult"
    joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; join 127.0.0.1:3016")
    echo "Join 1 done"

    clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
    echo "Final Primary: $clusterStatePrimary"

    joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; join 127.0.0.1:3018")
    echo "Join 2 done"
    sleep $timeoutSpeep
    clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
    echo "Final Primary: $clusterStatePrimary"
    sleep $timeoutSpeep
    clusterStateSecoundary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth $user $user; cluster-state;")
    echo "Final Secoundary: $clusterStateSecoundary"

    clusterStateSecoundary2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "auth $user $user; cluster-state;")
    echo "Final Secoundary2: $clusterStateSecoundary2"
    sleep 1
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

if [ $command = "save-admin" ] || [ $command = "all" ]
then
    RUST_BACKTRACE=1 ./target/debug/nun-db -p $password -u $user --host "http://$primaryHttpAddress" exec "auth mateus mateus; use-db \$admin mateus; snapshot;"
fi

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
;cluster-state  127.0.0.1:3016:Secoundary, 127.0.0.1:3018:Primary,"
    echo "Cluster state $clusterStatePrimary - $expectedCluster"

    if [ "$clusterStatePrimary" !=  "$expectedCluster" ]; then
        echo "Invalid cluster state after kill"
        exit 4
    else
        echo "Request Ok"
    fi
    rm .primary.pid
    rm .secoundary.pid
fi

exit 0

