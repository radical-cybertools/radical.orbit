FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=True
ENV PATH="/opt/ve_endpoint/bin:$PATH"
ENV RADICAL_ORBIT_BRIDGE_CERT="/app/private/bridge_cert.pem"
ENV RADICAL_ORBIT_BRIDGE_KEY="/app/private/bridge_key.pem"

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        gnupg \
        jq \
        openssl \
        python3-dev \
        sudo && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/ve_endpoint

WORKDIR /app

ARG RADICAL_ORBIT_BRANCH=master
RUN git clone -b ${RADICAL_ORBIT_BRANCH} https://github.com/radical-cybertools/radical.orbit.git

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir /app/radical.orbit

ARG GENERATE_BRIDGE_CERT=false
ARG BRIDGE_IP=127.0.0.1
ARG BRIDGE_HOSTNAME=localhost
RUN if [ "$GENERATE_BRIDGE_CERT" = "true" ]; then \
        mkdir -p $(dirname "$RADICAL_ORBIT_BRIDGE_CERT") && \
        openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout "${RADICAL_ORBIT_BRIDGE_KEY}" \
            -out "${RADICAL_ORBIT_BRIDGE_CERT}" \
            -subj "/CN=${BRIDGE_IP}" \
            -addext "subjectAltName = IP:${BRIDGE_IP},DNS:${BRIDGE_HOSTNAME},IP:127.0.0.1,DNS:localhost"; \
        echo "Certificate generated for ${BRIDGE_HOSTNAME} and ${BRIDGE_IP}"; \
    else \
        echo "Skipping certificate generation (GENERATE_BRIDGE_CERT is not true)."; \
    fi

CMD ["bash"]
