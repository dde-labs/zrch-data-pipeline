# ZRCH: Data Pipeline

**ZRCH** Assignment for create Data Pipeline.

> You are tasked with designing a data pipeline for a fictional e-commerce company,
> "ShopSmart". The company wants to analyze customer behavior, sales performance,
> and inventory management. You need to create a solution that ingests, processes,
> and stores data from various sources.

## Architecture

> [!NOTE]
> This it the first assignment that want to create end-to-end data pipeline from
> source to serve analytic reports. So, I will try to orchestrate with **Airflow**
> in this assignment.

## Getting Started

First, you should start with create your Python venv and install `uv`.

```shell
python -m venv venv
./venv/Scripts/activate
(venv) python -m pip install -U pip
(venv) pip install uv
```

Next, start provisioning all services by Docker Compose;

```shell
docker compose -f ./.container/docker-compose.warehouse.yml --env-file .env up -d
docker compose -f ./.container/docker-compose.yml --env-file .env up -d
```

> [!NOTE]
> Down Docker Compose;
> ```shell
> docker compose -f ./.container/docker-compose.yml --env-file .env down -v
> docker compose -f ./.container/docker-compose.warehouse.yml --env-file .env down
> ```