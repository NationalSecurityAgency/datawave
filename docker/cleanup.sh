#!/bin/sh
docker volume rm docker_quickstart_data
docker image prune -f
docker system prune -f
sudo find logs -type f -name '*log*' -delete
