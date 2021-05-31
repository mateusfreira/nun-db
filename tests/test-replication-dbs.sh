#!/bin/bash
trap "kill 0" EXIT

echo "Starting the primary"
primaryHttpAddress="127.0.0.1:9092"
primaryTcpAddress="127.0.0.1:3017"
secoundary1HttpAddress="127.0.0.1:9093"
secoundary2HttpAddress="127.0.0.1:9094"
user="mateus"
password="$user"
timeoutSpeep=0.1
RUST_BACKTRACE=0
command=$1

cargo build
echo "Add trap if all"
trap "kill 0" EXIT
echo "Will clean up the dbs"
./tests/commons.sh kill 2> /dev/null
./tests/commons.sh clean 2> /dev/null
sleep $timeoutSpeep

./tests/commons.sh start-1
./tests/commons.sh start-2
./tests/commons.sh start-3

sleep $timeoutSpeep
sleep $timeoutSpeep
sleep $timeoutSpeep

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
    fi
    if [ "$get_result2" != "empty;value jose-$i-1" ]; then
        echo "Invalid value value in the secoundary 2: $get_result $i"
        exit 3
    fi 
    echo "Request $i Ok"
    echo "Delete testing"

	deleteResult=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; remove state jose-$i-1;")
	echo "Read from the secoundary"
	get_result=$(curl -s -X "POST" "$secoundary1HttpAddress" -d "use-db test-db test-db-key; get state")
	get_result2=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")
	if [ "$get_result" != "empty;value <Empty>" ]; then
		echo "Invalid value value in the secoundary 1: $get_result $i"
		exit 2
	fi
    if [ "$get_result2" != "empty;value <Empty>" ]; then
        echo "Invalid value value in the secoundary 2: $get_result $i"
        exit 3
    fi 
	echo "Request $i Ok"
done
end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
echo "Total of $elapsed seconds elapsed for process"


echo "Will start the tests of failure"
cat .secoundary.pid | head -1 | xargs -I '{}' kill -9 {}
sleep $timeoutSpeep

echo 'Rebuilding the cluster'
sleep $timeoutSpeep

clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
echo $clusterStatePrimary


r=$(curl -s -X "POST" "$primaryHttpAddress" -d "use-db test-db test-db-key; set state $user;")
get_result=$(curl -s -X "POST" "$secoundary2HttpAddress" -d "use-db test-db test-db-key; get state")

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

clusterStatePrimary=$(curl -s -X "POST" "$primaryHttpAddress" -d "auth $user $user; cluster-state;")
expectedCluster="valid auth
;cluster-state  127.0.0.1:3017:Primary, 127.0.0.1:3018:Secoundary,"
echo "Cluster state $clusterStatePrimary - $expectedCluster"

if [ "$clusterStatePrimary" !=  "$expectedCluster" ]; then
    echo "Invalid cluster state after kill"
    exit 4
else
    echo "Request Ok"
fi
echo "========================================This is great success \o/!!!!!======================="

echo "Will clean up the dbs"
./tests/commons.sh kill 2>/dev/null
./tests/commons.sh clean 2>/dev/null

exit 0
