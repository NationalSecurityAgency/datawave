#!/bin/sh
services=$@
for service in $services; do
  docker compose stop $service
done
docker compose rm -f
docker compose --profile modification up -d
