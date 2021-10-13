#!/bin/sh
docker image prune -f
docker system prune -f
find logs -type f -name '*log*' -delete
