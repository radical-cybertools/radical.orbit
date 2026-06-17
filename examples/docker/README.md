# ORBIT — Docker Example

This directory contains a `Dockerfile` and `docker-compose.yaml` to run all
ORBIT endpoints (bridge, endpoint service, and client) inside separate
Docker containers.

> [!NOTE]
> We use the `dev` tag for the latest, but possibly unstable, configuration of
> the ORBIT image.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and
  [Docker Compose](https://docs.docker.com/compose/) installed.

## Steps

### 1. Set environment variables

```shell
export RADICAL_ORBIT_IMAGE=radicalcybertools/radical.orbit
export RADICAL_ORBIT_TAG=dev
# for the demo we use the current `devel` branch
export RADICAL_ORBIT_BRANCH=devel
# for the demo we use the hostname for the bridge as `bridge`
export RADICAL_BRIDGE_HOSTNAME=bridge
```

### 2. Build the image

The build step also generates a self-signed TLS certificate used by the bridge
endpoint.

> [!WARNING]
> The self-signed certificate is for **development purposes only**.

```shell
cd examples/docker
docker build --build-arg GENERATE_BRIDGE_CERT=true \
             --build-arg BRIDGE_IP=127.0.0.1 \
             --build-arg BRIDGE_HOSTNAME=${RADICAL_BRIDGE_HOSTNAME} \
             --build-arg RADICAL_ORBIT_BRANCH=${RADICAL_ORBIT_BRANCH} \
             -t ${RADICAL_ORBIT_IMAGE}:${RADICAL_ORBIT_TAG} .
```

### 3. Start containers and run the example

```shell
# start the bridge, endpoint, and client containers in the background
docker compose up -d

# get into the client container and run the example
docker exec -it orbit-client bash

cd /app/radical.orbit/examples
python3 example_sysinfo.py
```

### 4. Browse the API

The bridge service exposes port `8000` to the host, so once the containers are
running you can open the API documentation directly in a web browser:

| URL | Description |
|-----|-------------|
| <https://localhost:8000/docs> | Swagger UI — interactive API explorer |
| <https://localhost:8000/redoc> | ReDoc — alternative API reference |

> [!NOTE]
> Your browser will show a TLS warning because a self-signed certificate is
> used. Click **Advanced → Proceed to localhost** (or equivalent) to continue.

> [!TIP]
> When registering a new endpoint service through the portal (e.g., via the
> `/register` endpoint), use the **internal Docker hostname** as the Bridge URL:
> ```
> https://bridge:8000
> ```
> where `bridge` is the value of `BRIDGE_HOSTNAME` argument used during the docker
> build (default: `bridge`). Using `localhost` here would resolve on the host
> machine, not inside the Docker network.

### 5. Useful commands

```shell
# follow logs from bridge and endpoint containers
docker compose logs -f bridge -f endpoint

# stop and remove all containers
docker compose down
# if you want to delete named volumes:
#   docker compose down -v
```

## Further Reading

- Full getting-started guide:
  [`docs/source/getting_started.md`](../../docs/source/getting_started.md)
- Local and remote run instructions are covered in sections **3.1** and **3.3**
  of the same document.
