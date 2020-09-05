#!/bin/bash
trap "kill 0" EXIT

echo "Starting the primary"
primaryHttpAddress="127.0.0.1:9092"
primaryTcpAddress="127.0.0.1:3017"
secoundary1HttpAddress="127.0.0.1:9093"
secoundary2HttpAddress="127.0.0.1:9094"
cargo build
RUST_BACKTRACE=1 ./target/debug/nun-db --user mateus -p mateus start --http-address "$primaryHttpAddress" --tcp-address "$primaryTcpAddress" --ws-address "0.0.0.0:3058">primary.log&
PRIMARY_PID=$!


echo "Starting secoundary 1"

RUST_BACKTRACE=1 ./target/debug/nun-db --user mateus -p mateus start --http-address "$secoundary1HttpAddress" --tcp-address "0.0.0.0:3016" --ws-address "0.0.0.0:3057">secoundary.log&
SECOUNDARY_PID=$!


echo "Starting secoundary 2"

./target/debug/nun-db --user mateus -p mateus start --http-address "$secoundary2HttpAddress" --tcp-address "0.0.0.0:3018" --ws-address "0.0.0.0:3059">secoundary.2.log&
SECOUNDARY_2_PID=$!

sleep 1
echo "Will Connect the secoundaries to the primary"
electionResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; election-win")
echo "Election result: $electionResult"
joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; join 127.0.0.1:3016")
echo "Join 1 done"
sleep 1
joinResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; join 127.0.0.1:3018")
echo "Join 2 done"
sleep 1
clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; cluster-state;")
sleep 1
clusterStateSecoundary=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "auth mateus mateus; cluster-state;")
echo "Final : $clusterStatePrimary"
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

echo "Change cluster state primary : $clusterStatePrimary"
# Todo must be the same in all the members of the clustrer
echo "Change cluster state secoundary : $clusterStateSecoundary"

curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; create-db test-db test-db-key;"
sleep 5

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
done
end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
echo "Total of $elapsed seconds elapsed for process"

echo "Will start the tests of failure"
sleep 5

kill -9 $SECOUNDARY_PID

r=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; set state mateus;")
get_result=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")

echo "Check the log..."
sleep 10

echo  "Result: $get_result"

if [ "$get_result" != "empty;value mateus" ]; then
    echo "Invalid value value in the secoundary 1."
    exit 2
fi


for i in {1..20}
do
    echo "Set in the primary"
    r=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; set state jose-$i-1;")
    echo "Read from the secoundary"
	get_result2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")
    if [ "$get_result2" != "empty;value jose-$i-1" ]; then
        echo "Invalid value value in the secoundary 2: $get_result $i"
        exit 3
    else
        echo "Request $i Ok"
    fi
done

clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth mateus mateus; cluster-state;")
expectedCluster="valid auth
;cluster-state  127.0.0.1:3017:Primary, 127.0.0.1:3018:Secoundary,"
echo "Cluster state $clusterStatePrimary - $expectedCluster"

if [ "$clusterStatePrimary" !=  "$expectedCluster" ]; then
    echo "Invalid cluster state after kill"
    exit 4
else
    echo "Request Ok"
fi

exit 0

