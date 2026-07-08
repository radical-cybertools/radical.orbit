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

WORKDIR /home/$MAMBA_USER

ARG RADICAL_ORBIT_BRANCH=master
RUN git clone --single-branch -b ${RADICAL_ORBIT_BRANCH} https://github.com/radical-cybertools/radical.orbit.git && \
    pip install --no-cache-dir ./radical.orbit

COPY --chown=$MAMBA_USER:$MAMBA_USER pools.json .
COPY --chown=$MAMBA_USER:$MAMBA_USER workflow.makeflow .

