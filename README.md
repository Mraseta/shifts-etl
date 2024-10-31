# shifts-etl

## Approach

Data is fetched from shifts API, prepared for insertion and then inserted into the database. Dataframes needed for KPI calculations are returned so that reading from the database is not necessary before calculating KPIs. After calculating KPIs, a dataframe is created and inserted into database.
For each function a unit test was written. Whole ETL is covered by an integration test.
ETL can be run with Python or with Docker.

## Getting Started

Besides chosen language and tools for ETL job, you will need `docker` and `docker-compose`.

Initialize & start shifts API and target Postgres database in the background
with

```bash
$ docker-compose up -d
```

An instance of pgAdmin is running at
[http://localhost:5050](http://localhost:5050) configured with following
login information:

- username: `pgadmin@smartcat.io`
- password: `pgadmin`

After you log in, add a new database server. Postgres server is available
under `postgres` hostname because `pgAdmin` and `postgres` servers are inside
same docker network.

## Running ETL

Navigate to etl folder with

```bash
$ cd etl
```

After that, create .env file using

```bash
$ cp .env.example .env
```

### Running ETL with Python

First install all necessary libraries with

```bash
$ pip install -r requirements.txt
```

After that run ETL with

```bash
$ python src/etl.py
```

### Running ETL with Docker

Build docker image with

```bash
$ docker build -t shifts-etl .
```

After that run ETL with

```bash
$ docker run --network host shifts-etl
```

### Running tests

To run tests, first you need to install pytest (only if you haven't installed libraries from requirements.txt)

```bash
$ pip install pytest
```

Then, run following command

```bash
$ python -m pytest .
```