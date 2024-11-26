#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE metastore;
    CREATE DATABASE superset;
    GRANT ALL PRIVILEGES ON DATABASE superset TO ${POSTGRES_USER};
    CREATE USER hive WITH ENCRYPTED PASSWORD 'hive';
    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;
    GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
EOSQL

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" -d "superset" <<-EOSQL
   GRANT ALL ON SCHEMA public TO ${POSTGRES_USER};
EOSQL