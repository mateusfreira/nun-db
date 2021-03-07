#!/bin/sh

curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db sample sample-pwd;"

for j in {1..10}
do 
    for i in {1..200}
    do
        curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db org-$i-$j key-$i; use-db org-$i-$j key-$i; set state jose; get state; set key-$i jose;"&
        pids[${i}]=$!
    done

    # echo "${pids[*]}"
    # wait for all pids
    for pid in ${pids[*]}; do
        wait $pid
    done
done
echo  "It is over!!!"
