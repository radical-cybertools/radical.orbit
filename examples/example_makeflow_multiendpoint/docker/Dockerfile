FROM mambaorg/micromamba:2.6.0

USER root

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
        gnupg \
        jq \
        openssl \
        procps \
        sudo \
        vim && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /app && chown $MAMBA_USER:$MAMBA_USER /app

USER $MAMBA_USER

ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=True
ENV ENV_NAME=base

COPY --chown=$MAMBA_USER:$MAMBA_USER docker/environment.yml /tmp/environment.yml
RUN micromamba install -y -n $ENV_NAME -f /tmp/environment.yml && \
    micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1
ENV PATH="/opt/conda/bin:${PATH}"

ENV RADICAL_ORBIT_BROKER_CERT="/app/private/broker_cert.pem"
ENV RADICAL_ORBIT_BROKER_KEY="/app/private/broker_key.pem"

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
        chmod 0600 "${RADICAL_ORBIT_BROKER_KEY}"; \
    else \
        echo "Skipping certificate generation (GENERATE_BROKER_CERT is not true)."; \
    fi

WORKDIR /home/$MAMBA_USER

ARG RADICAL_ORBIT_BRANCH=master
RUN git clone --single-branch -b ${RADICAL_ORBIT_BRANCH} https://github.com/radical-cybertools/radical.orbit.git && \
    pip install --no-cache-dir ./radical.orbit

COPY --chown=$MAMBA_USER:$MAMBA_USER pools.json .radical/orbit/task_dispatcher/pools.json
COPY --chown=$MAMBA_USER:$MAMBA_USER workflow.makeflow .

