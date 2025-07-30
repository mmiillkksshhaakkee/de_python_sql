#!/bin/bash

docker run -d --name test \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v "$(pwd)/ny_taxi_pg_data:/var/lib/postgresql/data" \
  -p 5432:5432 \
  postgres:13

echo "waiting for postgres to become active..."
while ! docker exec test pg_isready -U postgres -h localhost > /dev/null 2>&1; do
  sleep 1
done

tmux new-session -d -s db_session
tmux send-keys -t db_session "source mv0/bin/activate && pgcli postgresql://root:root@localhost:5432/ny_taxi" C-m
tmux attach -t db_session