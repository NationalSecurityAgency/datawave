#!/bin/sh
docker image prune -f
docker system prune -f
find logs -name '*.log' -delete
