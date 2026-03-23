#!/bin/bash

# Uso: ./generar-compose.sh docker-compose-dev.yaml 5
# $1 = nombre de archivo de salida
# $2 = cantidad de clientes

set -e

outfile="$1"
num_clients="$2"

# Definición fija del servidor (sacada del docker-compose-dev.yaml original)
cat > "$outfile" <<EOF
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    volumes:
      - ./server/config.ini:/config.ini:ro
    networks:
      - testing_net
EOF

# Agrego los clientes client1, client2, ...
for i in $(seq 1 "$num_clients"); do
  cat >> "$outfile" <<EOF

  client$i:
    container_name: client$i
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=$i
      - CLI_LOG_LEVEL=DEBUG
    volumes:
      - ./client/config.yaml:/config.yaml:ro
    networks:
      - testing_net
    depends_on:
      - server
EOF
done

# Definición de la red (igual a la original)
cat >> "$outfile" <<EOF

networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOF
