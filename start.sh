#!/bin/bash

"""docker network inspect pg-admin-database >/dev/null 2>&1 || docker network create pg-admin-database

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

URL='https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' """
source ~/venv_ny_taxi/bin/activate
python3 generate_datasets.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=analytics_dashboard \
 --table_name='users products transactions user_actions' \
 --url=${URL}

echo "waiting for postgres to become active..."
while ! docker exec pg-database pg_isready -U postgres -h localhost > /dev/null 2>&1; do
  sleep 2
done

tmux new-session -d -s db_session
tmux send-keys -t db_session "bash -c 'source ~/venv_ny_taxi/bin/activate && pgcli postgresql://root:root@localhost:5432/ny_taxi 2>&1 | tee ~/db_session_error.log|| sleep 10'" Enter
tmux attach -t db_session
