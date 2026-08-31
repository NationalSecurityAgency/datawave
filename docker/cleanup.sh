#!/usr/bin/env bash
if [[ "${@/keepdata}" == "$@" ]]; then
  docker compose down --volumes --remove-orphans
else
  docker compose down --remove-orphans
fi
docker image prune -f
if [[ "${@/keeplog}" == "$@" ]]; then
  sudo find logs -type f -name '*log*' -delete
fi
