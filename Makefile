.PHONY: up down test

up:
	docker-compose up --build -d

down:
	docker-compose down

test:
	docker-compose run --rm backend pytest -p no:cacheprovider
