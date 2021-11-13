#!/bin/bash
trap "kill 0" EXIT

echo "Starting the primary"
primaryHttpAddress="127.0.0.1:9092"
primaryTcpAddress="127.0.0.1:3017"
secoundary1HttpAddress="127.0.0.1:9093"
secoundary2HttpAddress="127.0.0.1:9094"
cargo build
RUST_BACKTRACE=1 ./target/debug/nun-db --user mateus -p mateus start --http-address "$primaryHttpAddress" --tcp-address "$primaryTcpAddress" --ws-address "127.0.0.1:3058">primary.log&
PRIMARY_PID=$!

echo "Starting secoundary 1"

RUST_BACKTRACE=1 ./target/debug/nun-db --user mateus -p mateus start --http-address "$secoundary1HttpAddress" --tcp-address "127.0.0.1:3016" --ws-address "127.0.0.1:3057">secoundary.log&
SECOUNDARY_PID=$!


echo "Starting secoundary 2"

./target/debug/nun-db --user mateus -p mateus start --http-address "$secoundary2HttpAddress" --tcp-address "127.0.0.1:3018" --ws-address "127.0.0.1:3059">secoundary.2.log&
SECOUNDARY_2_PID=$!
sleep 5

echo "Will Connect the secoundaries to the primary"
echo "Election result: $electionResult"
joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; join 127.0.0.1:3016")

echo "Join 1 done"

clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; cluster-state;")
echo "Final Primary: $clusterStatePrimary"

joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; join 127.0.0.1:3018")
echo "Join 2 done"
sleep 1
clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; cluster-state;")
echo "Final Primary: $clusterStatePrimary"
sleep 10
clusterStateSecoundary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth mateus mateus; cluster-state;")
echo "Final Secoundary: $clusterStateSecoundary"

clusterStateSecoundary2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "auth mateus mateus; cluster-state;")
echo "Final Secoundary2: $clusterStateSecoundary2"
sleep 1
clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; cluster-state;")
clusterStateSecoundary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth mateus mateus; cluster-state;")
clusterStateSecoundary2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "auth mateus mateus; cluster-state;")

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

echo "Ready go test!!!!"
sleep 5000
