FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=True
ENV PATH="/opt/ve_endpoint/bin:$PATH"
ENV RADICAL_ORBIT_BROKER_CERT="/app/private/broker_cert.pem"
ENV RADICAL_ORBIT_BROKER_KEY="/app/private/broker_key.pem"

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

ARG GENERATE_BROKER_CERT=false
ARG BROKER_IP=127.0.0.1
ARG BROKER_HOSTNAME=localhost
RUN if [ "$GENERATE_BROKER_CERT" = "true" ]; then \
        mkdir -p $(dirname "$RADICAL_ORBIT_BROKER_CERT") && \
        openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout "${RADICAL_ORBIT_BROKER_KEY}" \
            -out "${RADICAL_ORBIT_BROKER_CERT}" \
            -subj "/CN=${BROKER_IP}" \
            -addext "subjectAltName = IP:${BROKER_IP},DNS:${BROKER_HOSTNAME},IP:127.0.0.1,DNS:localhost"; \
        echo "Certificate generated for ${BROKER_HOSTNAME} and ${BROKER_IP}"; \
    else \
        echo "Skipping certificate generation (GENERATE_BROKER_CERT is not true)."; \
    fi

CMD ["bash"]
