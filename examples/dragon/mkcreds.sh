#!/usr/bin/env bash
#
# Create the broker TLS cert/key and the ingress token for the compose
# setup in ./creds (ORBIT itself never generates credentials).  Existing
# files are kept.  Usage:  ./mkcreds.sh [broker-hostname]

set -euo pipefail

cd "$(dirname "$0")"
host="${1:-broker}"
mkdir -p creds
chmod 700 creds

if [[ ! -f creds/broker_key.pem ]]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:4096 \
        -keyout creds/broker_key.pem -out creds/broker_cert.pem \
        -subj "/CN=${host}" \
        -addext "subjectAltName = DNS:${host},DNS:localhost,IP:127.0.0.1" \
        2>/dev/null
    chmod 600 creds/broker_key.pem
    echo "created creds/broker_cert.pem, creds/broker_key.pem"
fi

if [[ ! -f creds/broker.token ]]; then
    (umask 077; python3 -c 'import secrets; print(secrets.token_urlsafe(32))' \
        > creds/broker.token)
    echo "created creds/broker.token"
fi
