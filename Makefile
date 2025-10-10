.PHONY: restart stop logs

restart:
	docker-compose down -v --remove-orphans
	docker-compose up -d --build

stop:
	docker-compose down -v

logs:
	docker-compose logs -f