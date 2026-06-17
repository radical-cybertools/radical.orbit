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

ENV RADICAL_BRIDGE_CERT="/app/private/bridge_cert.pem"
ENV RADICAL_BRIDGE_KEY="/app/private/bridge_key.pem"

ARG GENERATE_BRIDGE_CERT=false
ARG BRIDGE_IP=127.0.0.1
ARG BRIDGE_HOSTNAME=localhost
RUN if [ "$GENERATE_BRIDGE_CERT" = "true" ]; then \
        mkdir -p $(dirname "$RADICAL_BRIDGE_CERT") && \
        openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout "${RADICAL_BRIDGE_KEY}" \
            -out "${RADICAL_BRIDGE_CERT}" \
            -subj "/CN=${BRIDGE_IP}" \
            -addext "subjectAltName = IP:${BRIDGE_IP},DNS:${BRIDGE_HOSTNAME},IP:127.0.0.1,DNS:localhost"; \
        echo "Certificate generated for ${BRIDGE_HOSTNAME} and ${BRIDGE_IP}"; \
    else \
        echo "Skipping certificate generation (GENERATE_BRIDGE_CERT is not true)."; \
    fi

WORKDIR /home/$MAMBA_USER

ARG RADICAL_EDGE_BRANCH=master
RUN git clone -b ${RADICAL_EDGE_BRANCH} https://github.com/radical-cybertools/radical.edge.git && \
    pip install --no-cache-dir ./radical.edge

COPY --chown=$MAMBA_USER:$MAMBA_USER pools.json .radical/edge/task_dispatcher/pools.json
COPY --chown=$MAMBA_USER:$MAMBA_USER workflow.makeflow .

