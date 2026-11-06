# ORBIT with the `dragon` plugin and dragonhpc 0.15.0 (Orbit-Dragon POC).
#
# One image for both roles (broker, endpoint).  Build from the repository
# root so the local source tree is installed:
#
#   docker build -f examples/dragon/Dockerfile -t radical-orbit-dragon .
#
# (docker compose in this directory does that; the build context is limited
# by Dockerfile.dockerignore.)

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONNOUSERSITE=True

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openssl procps && \
    rm -rf /var/lib/apt/lists/*

# dragonhpc's wheel lacks `requests` (needed by dragon.workflows)
RUN pip install --no-cache-dir "dragonhpc==0.15.0" requests

WORKDIR /src/radical.orbit
COPY VERSION setup.py setup.cfg pyproject.toml requirements.txt MANIFEST.in \
     README.md LICENSE.md ./
COPY bin ./bin
COPY src ./src
RUN pip install --no-cache-dir .

WORKDIR /root
CMD ["bash"]
