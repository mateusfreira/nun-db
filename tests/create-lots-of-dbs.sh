#!/bin/sh

curl -X "POST" "http://localhost:9092" -d "auth mateus mateus; create-db sample sample-pwd;"

for j in {1..20}
do 
    for i in {1..100}
    do
        curl -X "POST" "http://localhost:9092" -d "auth mateus mateus; create-db org-$i-$j key-$i; use-db org-$i-$j key-$i; set state jose; get state; set key-$i jose;"&
        pids[${i}]=$!
    done

    # echo "${pids[*]}"
    # wait for all pids
    for pid in ${pids[*]}; do
        wait $pid
    done
done
echo  "It is over!!!"
