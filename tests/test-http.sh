#!/bin/sh

curl -X "POST" "http://localhost:3013" -d "auth mateus mateus; create-db sample sample-pwd; use-db sample sample-pwd; set state jose; get state"
