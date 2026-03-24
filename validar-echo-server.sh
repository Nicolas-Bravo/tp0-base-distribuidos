#!/bin/bash

set -euo pipefail

MESSAGE="test-echo-$(date +%s)"
NETWORK="tp0_testing_net"
SERVER_SERVICE="server"

# Envío el mensaje desde un contenedor auxiliar con netcat
RESPONSE=$(docker run --rm --network=${NETWORK} alpine /bin/sh -c "echo '${MESSAGE}' | nc ${SERVER_SERVICE} 12345")

if [ "${RESPONSE}" = "${MESSAGE}" ]; then
  echo "action: test_echo_server | result: success"
else
  echo "action: test_echo_server | result: fail"
fi

  exit 0
