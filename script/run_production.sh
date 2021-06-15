#!/bin/bash

chmod 644 docker/config/metricbeat/production/metricbeat.yml

IP=$(curl https://ipinfo.io/ip)

rm .env
echo "PUBLIC_IP=$IP" > .env

docker-compose -f  docker/docker-compose-production.yml  --env-file .env up -d --build

exit 0