#!/usr/bin/bash

docker volume rm sonarqube_data
docker volume rm sonarqube_extensions
docker volume rm sonarqube_logs
docker volume rm sonarqube_postgresql
docker volume rm sonarqube_postgresql_data
docker volume rm sonarqube_sonarqube_data
docker volume rm sonarqube_sonarqube_extensions
docker volume rm sonarqube_sonarqube_logs

