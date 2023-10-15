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
