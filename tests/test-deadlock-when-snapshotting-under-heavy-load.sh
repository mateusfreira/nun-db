#!/bin/sh

 time for j in {1..10000}; do ./target/debug/nun-db -p mateus -u mateus exec "use-db sample sample-pwd;keys;set jose$j $j;get jose;remove jose; get jose; snapshot "; done

