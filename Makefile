connect-logs:
	docker logs -f connect

ksql-logs:
	docker logs -f ksqldb-server

buildStreams:
	cd streams && gradle shadowJar

up: 
	docker-compose up -d

down: 
	docker-compose down

restart-connect:
	docker-compose restart connect

restart-ksql:
	docker-compose restart ksqldb-server

sh-ksqldb:
	docker-compose exec ksqldb-server /bin/sh ksql

sh-connect:
	docker-compose exec connect /bin/sh