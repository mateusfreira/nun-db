# Variables
primaryTcpAddress="127.0.0.1:3017"
primaryHttpAddress = "127.0.0.1:9092"
secoundary1HttpAddress = "127.0.0.1:9093"
secoundary2HttpAddress = "127.0.0.1:9094"
user = "mateus"
password = $(user)
timeoutSpeep = 3
replicaSetAddrs = "127.0.0.1:3016,127.0.0.1:3017,127.0.0.1:3018"

# ../../targets and Rules
build:
	cargo build

snapshot-all: build
	@echo "Snapshot all dbs"
	(RUST_BACKTRACE=1 ./../../target/debug/nun-db --user $(user) -p $(password) --host "http://$(primaryHttpAddress)" exec "use-db \$admin $(password); keys" | tr "," "\n" | sort | grep -v "lost+found" | tail +5 | xargs -I '{}' ./target/debug/nun-db --user $(user) -p $(password) --host "http://$(primaryHttpAddress)" exec "replicate-snapshot {}") &> /dev/null

kill:
	cat .primary.pid | xargs -I '{}' kill -9 {}
	cat .secoundary.pid | xargs -I '{}' kill -9 {}

clean:
	@echo "Clean!" 
	@rm .secoundary.pid 2> /dev/null || true
	@rm .primary.pid 2> /dev/null || true
	@rm /tmp/dbs/* 2> /dev/null || true
	@rm /tmp/dbs1/* 2> /dev/null || true
	@rm /tmp/dbs2/* 2> /dev/null || true
	@mkdir /tmp/dbs/ 2> /dev/null || true
	@mkdir /tmp/dbs1/ 2> /dev/null || true
	@mkdir /tmp/dbs2/ 2> /dev/null || true

start-1: build
	@echo "Starting the primary"
	NUN_DBS_DIR=/tmp/dbs RUST_BACKTRACE=1 ./../../target/debug/nun-db --user $(user) -p $(user) start --http-address "$(primaryHttpAddress)" --tcp-address "$(primaryTcpAddress)" --ws-address "127.0.0.1:3058" --replicate-address "$(replicaSetAddrs)" >> primary.log &
	echo $$! >> .primary.pid
	sleep $(timeoutSpeep)

cluster-state:
	@curl -s -X "POST" "$(primaryHttpAddress)" -d "auth $(user) $(user); cluster-state;"



start-2:
	@echo "Starting secoundary 1"
	NUN_DBS_DIR=/tmp/dbs1 RUST_BACKTRACE=1 ./../../target/debug/nun-db --user $(user) -p $(user) start --http-address "$(secoundary1HttpAddress)" --tcp-address "127.0.0.1:3016" --ws-address "127.0.0.1:3057" --replicate-address "$(replicaSetAddrs)" >> secoundary.log &
	echo $$! >> .secoundary.pid
	sleep $(timeoutSpeep)

start-3:
	@echo "Starting secoundary 2"
	NUN_DBS_DIR=/tmp/dbs2 RUST_BACKTRACE=1 ./../../target/debug/nun-db --user $(user) -p $(user) start --http-address "$(secoundary2HttpAddress)" --tcp-address "127.0.0.1:3018" --ws-address "127.0.0.1:3059" --join-address "localhost:3018" --replicate-address "$(replicaSetAddrs)" >> secoundary.2.log &
	echo $$! >> .secoundary.pid
	sleep $(timeoutSpeep)

start-all: start-1 start-2 start-3

save-admin: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "auth mateus mateus; use-db \$admin mateus; snapshot;"

print-election-primary:
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "cluster-state;"

print-election-2: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(secoundary1HttpAddress)" exec "cluster-state;"

print-election-3: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(secoundary2HttpAddress)" exec "cluster-state;"

create-vue: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "auth mateus mateus; create-db vue vue_pwd; use-db vue vue_pwd; snapshot;"

create-sample: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "auth mateus mateus; create-db sample sample-pwd; use-db sample sample-pwd; snapshot;"

create-test:
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "auth mateus mateus; create-db test test-pwd arbiter; use-db test test-pwd; snapshot;"

create-blog: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(primaryHttpAddress)" exec "auth mateus mateus; create-db analitcs-blog analitcs-blog-2903uyi9ewrj; use-db analitcs-blog analitcs-blog-2903uyi9ewrj; snapshot;"

set-on-secoundary: build
	RUST_BACKTRACE=1 ./../../target/debug/nun-db -p $(password) -u $(user) --host "http://$(secoundary1HttpAddress)" exec "use-db test test-pwd; set name from-the-test;"

kill-all:
	killall -9 nun-db


# Add a "all" target
all: build snapshot-all kill clean start-1 cluster-state start-2 start-3 save-admin print-election-primary print-election-2 print-election-3 create-vue create-sample create-test create-blog set-on-secoundary

.PHONY: build snapshot-all kill clean start-1 cluster-state start-2 start-3 save-admin print-election-primary print-election-2 print-election-3 create-vue create-sample create-test create-blog set-on-secoundary

connect-primary:
	@echo "Connecting to primary"
	@nc localhost 3017

ps:
	@ps aux | grep nun-db
