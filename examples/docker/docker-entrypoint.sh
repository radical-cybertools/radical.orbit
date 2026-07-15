#!/usr/bin/env bash

# docker-entrypoint.sh
#
# Runs at container start for every ORBIT role (broker, endpoint, client).
# Sources ~/.radical/orbit/init.env (baked into the image at build time) for
# structural parameters (BROKER_IP, BROKER_HOSTNAME, cert/key paths), then
# checks the runtime env var GENERATE_BROKER_CREDS.  When set to "true" (broker
# role only), fresh TLS credentials and an ingress token are generated on every
# container start; any stale files from a previous run are removed first.

set -euo pipefail

INIT_ENV="${HOME}/.radical/orbit/init.env"

# ── load settings ──────────────────────────────────────────────────────────────
if [[ -f "$INIT_ENV" ]]; then
    source "$INIT_ENV"
fi

# ── (re)generate credentials ───────────────────────────────────────────────────
private_dir="$(dirname "${RADICAL_ORBIT_BROKER_CERT}")"
mkdir -p "${private_dir}" "${HOME}/.radical/orbit"

if [[ "${GENERATE_BROKER_CREDS:-false}" == "true" ]]; then

    # Remove stale files so every restart produces fresh credentials.
    rm -f "${RADICAL_ORBIT_BROKER_CERT}" \
          "${RADICAL_ORBIT_BROKER_KEY}"  \
          "${private_dir}/broker.token"  \
          "${HOME}/.radical/orbit/broker.token"

    # TLS certificate + private key
    openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
        -keyout "${RADICAL_ORBIT_BROKER_KEY}" \
        -out "${RADICAL_ORBIT_BROKER_CERT}" \
        -subj "/CN=${BROKER_IP}" \
        -addext "subjectAltName = IP:${BROKER_IP},DNS:${BROKER_HOSTNAME},IP:127.0.0.1,DNS:localhost" \
        2>/dev/null
    chmod 0600 "${RADICAL_ORBIT_BROKER_KEY}"

    # Ingress token
    python3 -c "import secrets; print(secrets.token_urlsafe(32))" > token.tmp
    chmod 600 token.tmp
    mv token.tmp "${private_dir}/broker.token"

    echo "[entrypoint] Certificate & token generated (${BROKER_HOSTNAME}:${BROKER_IP})."

else
    echo "[entrypoint] Skipping certificate & token generation."
fi

if [[ -f "${private_dir}/broker.token" ]]; then
    ln -sf "${private_dir}/broker.token" "${HOME}/.radical/orbit/broker.token"
fi

# ── hand off to the requested command ─────────────────────────────────────────
exec "$@"
