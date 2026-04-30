#!/bin/bash
# Creates additional databases listed in POSTGRES_MULTIPLE_DATABASES (comma-separated).
# The default database (POSTGRES_DB) is already created by the postgres image.
set -e

function create_db() {
    local database=$1
    echo "  → creating database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Creating extra databases: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
        create_db "$db"
    done
    echo "Done."
fi
