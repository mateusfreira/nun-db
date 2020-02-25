#!/bin/sh

curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db http-test jose; use-db http-test jose; set state jose; get state"
