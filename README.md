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
### Overview
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


### Deployment

To build new image, go to **stream/** directory and run:
```sh
docker build -t puska-farm-etl-stream .
```

There are two ways to run ETL services:

#### Session Mode

Create container from ETL and set to idle mode:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5601 \
    -e DWH_DATABASE=puska \
    --name puska-farm-etl-stream \
    puska-farm-etl-stream:latest \
    sleep infinity
```

Run choosen ETL with `exec` command:
```sh
docker exec -it puska-farm-etl-stream python -m etl.seq_fact_distribusi.run
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    etl-stream:
        build:
            context: .
        environment:
            - DWH_USERNAME=${DWH_USERNAME:-puska}
            - DWH_PASSWORD=${DWH_PASSWORD:-puska}
            - DWH_HOSTNAME=${DWH_HOSTNAME:-localhost}
            - DWH_PORT=${DWH_PORT:-5601}
            - DWH_DATABASE=${DWH_DATABASE:-puska}
        entrypoint: ["sleep", "infinity"]
```

Run choosen ETL with `exec` command:
```sh
docker compose exec -it etl-stream python -m etl.seq_fact_distribusi.run
```


#### Standalone

Create container as single run jobs by specifying runtime command:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5432 \
    -e DWH_DATABASE=puska \
    --name puska-farm-etl-stream-distribusi \
    puska-farm-etl-stream:latest \
    python -m etl.seq_fact_distribusi
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    distribusi:
        build:
            context: .
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
docker compose up distribusi
```


## Batch ETL
### Overview
Batch data ETL for scheduled summarization. Each ETL will follow this workflow:
1. Query data from Source DB (Ops)
2. Transform data to DWH format
3. If success, load data to DWH and logging
4. If failed, raise error and logging

ETL List
|Name|Description|
|:--|:--|
|seq_fact_distribusi|Processing farm products distribution summary|
|seq_fact_populasi|Processing farm livestocks population summary|
|seq_fact_produksi|Processing farm products production summary|

For Batch ETL runtime, several environment variables must be set:
|Name|Description|
|:--|:--|
|DWH_USERNAME|DWH user username|
|DWH_PASSWORD|DWH user password|
|DWH_HOSTNAME|DWH hostname|
|DWH_PORT|DWH port|
|OPS_DATABASE|Ops database name|
|OPS_USERNAME|Ops user username|
|OPS_PASSWORD|Ops user password|
|OPS_HOSTNAME|Ops hostname|
|OPS_PORT|Ops port|
|OPS_DATABASE|Ops database name|


### Deployment

To build new image, go to **batch/** directory and run:
```sh
docker build -t puska-farm-etl-batch .
```

There are two ways to run ETL services:

#### Session Mode

Create container from ETL and set to idle mode:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5601 \
    -e DWH_DATABASE=puska \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5600 \
    -e DWH_DATABASE=puska \
    --name puska-farm-etl-batch \
    puska-farm-etl-batch:latest \
    sleep infinity
```

Run choosen ETL with `exec` command:
```sh
docker exec -it puska-farm-etl-batch python -m etl.seq_fact_distribusi.run
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    etl-batch:
        build:
            context: .
        environment:
            - DWH_USERNAME=${DWH_USERNAME:-puska}
            - DWH_PASSWORD=${DWH_PASSWORD:-puska}
            - DWH_HOSTNAME=${DWH_HOSTNAME:-localhost}
            - DWH_PORT=${DWH_PORT:-5601}
            - DWH_DATABASE=${DWH_DATABASE:-puska}
            - OPS_USERNAME=${OPS_USERNAME:-puska}
            - OPS_PASSWORD=${OPS_PASSWORD:-puska}
            - OPS_HOSTNAME=${OPS_HOSTNAME:-localhost}
            - OPS_PORT=${OPS_PORT:-5600}
            - OPS_DATABASE=${OPS_DATABASE:-puska}
        entrypoint: ["sleep", "infinity"]
```

Run choosen ETL with `exec` command:
```sh
docker compose exec -it etl-batch python -m etl.seq_fact_distribusi.run
```


#### Standalone

Create container as single run jobs by specifying runtime command:
```sh
docker run \
    -e DWH_USERNAME=puska \
    -e DWH_PASSWORD=puska \
    -e DWH_HOSTNAME=localhost \
    -e DWH_PORT=5601 \
    -e DWH_DATABASE=puska \
    -e OPS_USERNAME=puska \
    -e OPS_PASSWORD=puska \
    -e OPS_HOSTNAME=localhost \
    -e OPS_PORT=5600 \
    -e OPS_DATABASE=puska \
    --name puska-farm-etl-batch-distribusi \
    puska-farm-etl-batch:latest \
    python -m etl.seq_fact_distribusi.run
```

For **docker compose**, specify ETL services with override `entrypoint` command:
```yml
version: "3"
services:
    distribusi:
        build:
            context: ./puska-farm-dwh/src/postgres
        environment:
            - DWH_USERNAME=${DWH_USERNAME:-puska}
            - DWH_PASSWORD=${DWH_PASSWORD:-puska}
            - DWH_HOSTNAME=${DWH_HOSTNAME:-localhost}
            - DWH_PORT=${DWH_PORT:-5601}
            - DWH_DATABASE=${DWH_DATABASE:-puska}
            - OPS_USERNAME=${OPS_USERNAME:-puska}
            - OPS_PASSWORD=${OPS_PASSWORD:-puska}
            - OPS_HOSTNAME=${OPS_HOSTNAME:-localhost}
            - OPS_PORT=${OPS_PORT:-5600}
            - OPS_DATABASE=${OPS_DATABASE:-puska}
        entrypoint: ["python", "-m", "etl.seq_fact_distribusi.run"]
```

To re-run job, simply `up` service name:
```sh
docker compose up distribusi
```