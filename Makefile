IMAGE_NAME = spark-tg-local
WORKDIR = /workspace

build-image:
	docker build -t $(IMAGE_NAME) .

shell:
	docker run --rm -it -v "$(PWD)":$(WORKDIR) -w $(WORKDIR) $(IMAGE_NAME) /bin/bash

sample-data:
	docker compose run --rm spark python create_sample_data.py

run-job:
	docker compose run --rm spark \
	  spark-submit \
	  --jars /opt/spark/jars-extra/tigergraph-spark-connector-0.2.4.jar \
	  jobs/adls_to_graph.py

up:
	docker compose up -d

down:
	docker compose down

tg-schema:
	docker compose run --rm spark python create_tg_schema.py