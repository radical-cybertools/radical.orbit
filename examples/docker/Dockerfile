FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=True

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        gnupg \
        jq \
        openssl \
        procps \
        python3-dev \
        sudo \
        vim && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python3 -m venv /opt/ve_endpoint

ENV PATH="/opt/ve_endpoint/bin:$PATH"

ARG RADICAL_ORBIT_BRANCH=master
RUN git clone --single-branch -b ${RADICAL_ORBIT_BRANCH} https://github.com/radical-cybertools/radical.orbit.git && \
    . /opt/ve_endpoint/bin/activate && \
    pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir /app/radical.orbit

ENV RADICAL_ORBIT_BROKER_CERT="/app/private/broker_cert.pem"
ENV RADICAL_ORBIT_BROKER_KEY="/app/private/broker_key.pem"

ARG GENERATE_BROKER_CERT=false
ARG BROKER_IP=127.0.0.1
ARG BROKER_HOSTNAME=localhost
RUN if [ "$GENERATE_BROKER_CERT" = "true" ]; then \
        private_dir=$(dirname "$RADICAL_ORBIT_BROKER_CERT"); \
        mkdir -p "${private_dir}" "${HOME}/.radical/orbit"; \
        openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout "${RADICAL_ORBIT_BROKER_KEY}" \
            -out "${RADICAL_ORBIT_BROKER_CERT}" \
            -subj "/CN=${BROKER_IP}" \
            -addext "subjectAltName = IP:${BROKER_IP},DNS:${BROKER_HOSTNAME},IP:127.0.0.1,DNS:localhost" && \
        chmod 0600 "${RADICAL_ORBIT_BROKER_KEY}"; \
        python3 -c "import secrets; print(secrets.token_urlsafe(32))" > token.tmp && \
            chmod 600 token.tmp && mv token.tmp "${private_dir}/broker.token" && \
            ln -s "${private_dir}/broker.token" "${HOME}/.radical/orbit/broker.token"; \
        echo "Certificate & token generated (${BROKER_HOSTNAME}, ${BROKER_IP})."; \
    else \
        echo "Skipping certificate & token generation (GENERATE_BROKER_CERT is not true)."; \
    fi

CMD ["bash"]
