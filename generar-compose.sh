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
    volumes:
      - ./server/config.ini:/config.ini
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
      - NOMBRE=Nombre$i
      - APELLIDO=Apellido$i
      - DOCUMENTO=1000000$i
      - NACIMIENTO=2000-01-01
      - NUMERO=7574
    volumes:
      - ./client/config.yaml:/config.yaml
      - ./data/agency-$i.csv:/data/agency-$i.csv
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
