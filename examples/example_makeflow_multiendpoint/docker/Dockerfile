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

ARG BROKER_IP=127.0.0.1
ARG BROKER_HOSTNAME=localhost
RUN mkdir -p ${HOME}/.radical/orbit && \
    { echo "BROKER_IP=${BROKER_IP}"; \
      echo "BROKER_HOSTNAME=${BROKER_HOSTNAME}"; \
      echo "RADICAL_ORBIT_BROKER_CERT=${RADICAL_ORBIT_BROKER_CERT}"; \
      echo "RADICAL_ORBIT_BROKER_KEY=${RADICAL_ORBIT_BROKER_KEY}"; \
    } > "${HOME}/.radical/orbit/init.env"

ARG DOCKER_ENTRYPOINT=docker-entrypoint.sh
COPY --chmod=755 ${DOCKER_ENTRYPOINT} /usr/local/bin/docker-entrypoint.sh

WORKDIR /home/$MAMBA_USER

ARG RADICAL_ORBIT_BRANCH=master
RUN git clone --single-branch -b ${RADICAL_ORBIT_BRANCH} https://github.com/radical-cybertools/radical.orbit.git && \
    pip install --no-cache-dir ./radical.orbit

COPY --chown=$MAMBA_USER:$MAMBA_USER pools.json .
COPY --chown=$MAMBA_USER:$MAMBA_USER workflow.makeflow .

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
