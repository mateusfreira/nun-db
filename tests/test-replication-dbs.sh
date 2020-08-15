#!/bin/bash
trap "kill 0" EXIT
# Run db main
 cargo run -- --user mateus -p mateus start  --tcp-address "0.0.0.0:3016" --ws-address "0.0.0.0:3057" --http-address "0.0.0.0:9091">secoundary.log&

# Run db 2
cargo run -- --user mateus -p mateus start  --tcp-address "0.0.0.0:3017" --ws-address "0.0.0.0:3058" --http-address "0.0.0.0:9092" --replicate-address "127.0.0.1:3016">primary.log&

sleep 10000
start_time="$(date -u +%s)"
for i in {1..2000}
do
    r=$(curl -s -X "POST" "http://localhost:9092" -d "auth mateus mateus; create-db org-$i key-$i; use-db org-$i key-$i; set state jose-$i-1;")
    get_result=$(curl -s -X "POST" "http://localhost:9091" -d "use-db org-$i key-$i;get state")
    if [ "$get_result" != "empty;value jose-$i-1" ]; then
       echo "Invalid value : $get_result $i"
       exit 1
    else
       echo "Server returned:"
    fi
done
end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
echo "Total of $elapsed seconds elapsed for process"
exit 0

