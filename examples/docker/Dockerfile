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

ARG BROKER_IP=127.0.0.1
ARG BROKER_HOSTNAME=localhost
RUN mkdir -p ${HOME}/.radical/orbit && \
    { echo "BROKER_IP=${BROKER_IP}"; \
      echo "BROKER_HOSTNAME=${BROKER_HOSTNAME}"; \
      echo "RADICAL_ORBIT_BROKER_CERT=${RADICAL_ORBIT_BROKER_CERT}"; \
      echo "RADICAL_ORBIT_BROKER_KEY=${RADICAL_ORBIT_BROKER_KEY}"; \
    } > "${HOME}/.radical/orbit/init.env"

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bash"]
