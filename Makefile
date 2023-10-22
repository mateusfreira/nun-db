docker-up:
	@docker-compose up -d

docker-up-toxicproxy:
	@docker-compose up -d toxicproxy toxicproxy-config

docker-up-primary:
	@docker-compose up -d nun-db-1

docker-up-secondary:
	@docker-compose up -d nun-db-secondary-1

docker-down:
	@docker-compose down


docker-logs-follower:
	@docker-compose logs -f

docker-exec-toxiproxy:
	@docker-compose exec toxiproxy sh


run-tests:
	@cargo test -- --test-threads=1 --show-output


add-primary-latency:
	@docker-compose exec toxiproxy sh -c "/go/bin/toxiproxy-cli -h toxiproxy:8474  toxic add -t latency -a latency=500 primary"

remove-primary-latency:
	@docker-compose exec toxiproxy sh -c "/go/bin/toxiproxy-cli -h toxiproxy:8474  toxic remove -n latency_downstream primary"

remove-all-latency:
	@docker-compose exec toxiproxy sh -c '/go/bin/toxiproxy-cli -h toxiproxy:8474  toxic remove -n latency_downstream primary & /go/bin/toxiproxy-cli -h toxiproxy:8474  toxic remove -n latency_downstream secondary-1 & /go/bin/toxiproxy-cli -h toxiproxy:8474  toxic remove -n latency_downstream secondary-2'


add-full-latency:
	@docker-compose exec toxiproxy sh -c "/go/bin/toxiproxy-cli -h toxiproxy:8474  toxic add -t latency -a latency=3000 primary&/go/bin/toxiproxy-cli -h toxiproxy:8474  toxic add -t latency -a latency=3000 secondary-1 & /go/bin/toxiproxy-cli -h toxiproxy:8474  toxic add -t latency -a latency=3000 secondary-2"
