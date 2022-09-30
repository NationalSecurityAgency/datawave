#!/bin/sh
if [[ "${@/keepdata}" == "$@" ]]; then
  docker volume rm docker_quickstart_data
fi
docker image prune -f
docker system prune -f
if [[ "${@/keeplog}" == "$@" ]]; then
  sudo find logs -type f -name '*log*' -delete
fi
