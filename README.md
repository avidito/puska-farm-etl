# Puska Farm - ETL

## Description
Puska Farm - ETL scripts definition. Deployed as containerized application. Built using **Python**, consists of:
1. Stream ETL, for streaming data processing
2. Batch ETL, for batching data processing

Both ETL will load data to Kafka (for Streaming BI) and Realtime DWH.

## Related Project
1. Puska Farm - DWH
2. Puska Farm - ML
3. Puska Farm - BI

## Stream ETL
Streaming data ETL for realtime summarization. Each ETL will follow this workflow:
1. Receive data from Kafka (Data)
2. Validate and Process data
3. If success, load data to DWH, logging and push to Kafka (BI)
4. If failed, raise error and logging

ETL List
|Name|Description|
|:--|:--|
|seq_fact_distribusi|Processing farm products distribution summary|
|seq_fact_populasi|Processing farm livestocks population summary|
|seq_fact_produksi|Processing farm products production summary|

For Stream ETL runtime, several environment variables must be set:
|Name|Description|
|:--|:--|
|DWH_USERNAME|DWH user username|
|DWH_PASSWORD|DWH user password|
|DWH_HOSTNAME|DWH hostname|
|DWH_PORT|DWH port|
|DWH_DATABASE|DWH database name|


## Deployment
This project directly derived from Python image. Below are the list of envi

To build new image, go to **stream/** directory and run:
```sh
docker build -t puska-farm-etl-stream .
```

or got to **/batch** directory and run:

```sh
docker build -t puska-farm-etl-batch .
```


There are two ways to run ETL services:

### Session Mode

Create container from ETL and set to idle mode:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5432 \
    -e DWH_DATABASE=puska \
    --name puska-farm-etl-stream \
    puska-farm-etl-stream:latest \
    sleep infinity
```

Run choosen ETL with `exec` command:
```sh
docker exec -it puska-farm-etl python -m etl.seq_fact_distribusi.run
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    etl-stream:
        build:
            context: ./stream
        environment:
            - DWH_USERNAME=${DWH_USERNAME:-puska}
            - DWH_PASSWORD=${DWH_PASSWORD:-puska}
            - DWH_HOSTNAME=${DWH_HOSTNAME:-localhost}
            - DWH_PORT=${DWH_PORT:-5432}
            - DWH_DATABASE=${DWH_DATABASE:-puska}
        entrypoint: ["sleep", "infinity"]
```

Run choosen ETL with `exec` command:
```sh
docker compose exec -it etl-stream python -m etl.seq_fact_distribusi.run
```


### Standalone

Create container as single run jobs by specifying runtime command:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5432 \
    -e DWH_DATABASE=puska \
    --name puska-farm-etl-stream-fact_distribusi \
    puska-farm-etl-stream:latest \
    python -m etl.seq_fact_distribusi
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    etl-stream-fact_distribusi:
        build:
            context: ./puska-farm-dwh/src/postgres
        environment:
            - DWH_USERNAME=${DWH_USERNAME:-puska}
            - DWH_PASSWORD=${DWH_PASSWORD:-puska}
            - DWH_HOSTNAME=${DWH_HOSTNAME:-localhost}
            - DWH_PORT=${DWH_PORT:-5432}
            - DWH_DATABASE=${DWH_DATABASE:-puska}
        entrypoint: ["python", "-m", "etl.seq_fact_distribusi.run"]
```

To re-run job, simply `up` service name:
```sh
docker compose up etl-stream-fact_distribusi
```