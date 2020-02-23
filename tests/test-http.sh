#!/bin/sh

curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db http-test; use-db http-test; set state jose-1; get state"
