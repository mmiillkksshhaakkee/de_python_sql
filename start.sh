#!/bin/bash

docker network inspect pg-admin-database >/dev/null 2>&1 || docker network create pg-admin-database

docker run -d \
  --network=pg-admin-database \
  --name=pg-database \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v "$(pwd)/ny_taxi_pg_data:/var/lib/postgresql/data" \
  -p 5432:5432 \
  postgres:13

docker run -d \
  --network=pg-admin-database \
  --name=pg_admin \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4

echo "waiting for postgres to become active..."
while ! docker exec pg-database pg_isready -U postgres -h localhost > /dev/null 2>&1; do
  sleep 2
done

tmux new-session -d -s db_session "bash -c 'source /home/PycharmProjects/de_zoomcamp/.venv/bin/activate && pgcli postgresql://root:root@localhost:5432/ny_taxi || sleep 10'"
tmux attach -t db_session
