#!/bin/sh
for i in {1..600}
do
    curl -X "POST" "http://localhost:9092" -d "auth mateus mateus; create-db org-$i key-$i; use-db org-$i key-$i; set state jose; get state; set key-$i jose;"
done

