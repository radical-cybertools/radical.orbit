# Getting Started with ORBIT

## Outline

1. [Installation](#1-installation)
2. [Configuration](#2-configuration)
3. [Run demo](#3-run-demo)

## 1. Installation

Prepare the environment with all necessary packages. We will use a virtual 
environment for all our endpoints (endpoint service, bridge, and client). For local 
runs it will be the same environment, while for production runs, each endpoint 
will have its own virtual environment.

> [!NOTE]
> Python requirements >= **3.10**

### 1.1. Create virtual environment

```shell
export PYTHONNOUSERSITE=True
python3 -m venv ve_endpoint
source ve_endpoint/bin/activate
```

### 1.2. Install packages

> [!NOTE]
> For this demo we use a `development` branch, which includes all the latest 
> changes, but it should be treated as an unstable release.

```shell
pip install git+https://github.com/radical-cybertools/radical.orbit.git@devel
# TO BE REPLACED by RHAPSODY and RADICAL-AsyncFlow
pip install git+https://github.com/radical-cybertools/radical.pilot.git@devel
```

### 1.3. Generate certificate

Run the following command on the machine which will serve as **the bridge 
endpoint**. This machine will hold the original self-signed certificate to 
allow the remote access.

> [!WARNING]
> We use self-signed certificate for the **development** purposes only!

```shell
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 \
            -keyout bridge_key.pem -out bridge_cert.pem \
            -subj "/CN=<IPv4>" \
            -addext "subjectAltName = IP:<IPv4>,DNS:localhost,IP:127.0.0.1"
```

Add every clients' address, for example:

```shell
 ... -addext "subjectAltName =
IP:95.217.193.116,
IP:10.0.0.5,
IP:127.0.0.1,
DNS:endpoint.example.org,
DNS:localhost"
```

In case you need to check the `IPv4` address(es) to use, please run the 
following python code to print it for each network interface on your machine. 
Different machines might have different network interface labels, but if you 
have one as `en0` or `eth0`, please use any address related to them.

```python
import socket
import psutil

for iface, net_ifs in psutil.net_if_addrs().items():
    for net_if in net_ifs:
        if net_if.family == socket.AF_INET:
            print(f'{iface}: {net_if.address}')
```

### 1.4. Get Endpoint repo (optional)

Get the GitHub repository to use it for test runs of the examples.

```shell
git clone https://github.com/radical-cybertools/radical.orbit.git
```

## 2. Configuration

The bridge endpoint should have environment variables `RADICAL_BRIDGE_CERT` and
`RADICAL_BRIDGE_KEY` to be set before it starts, while the endpoint service requires 
to have `RADICAL_BRIDGE_CERT` only.

```shell
export RADICAL_BRIDGE_CERT=`pwd`/bridge_cert.pem
export RADICAL_BRIDGE_KEY=`pwd`/bridge_key.pem
```

Endpoint service and client endpoints should be provided with the bridge url, 
either as an argument or as the environment variable (e.g., 
`export RADICAL_BRIDGE_URL='https://localhost:8000'`).

## 3. Run demo

### 3.1. Local run

All endpoints share the same target machine. Use different terminals to 
start/run a corresponding endpoint/script.

#### 3.1.A. Terminal 1 (bridge)

Run the bridge endpoint which bridges between the client and the endpoint service.

```shell
# corresponding virtual environment (e.g., ve_endpoint) should be active,
# env variables RADICAL_BRIDGE_CERT, RADICAL_BRIDGE_KEY should be set
orbit-bridge.py
```

Example output:
```text
[Bridge] URL: https://localhost:8000/register
INFO:     Started server process [1]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on https://0.0.0.0:8000 (Press CTRL+C to quit)
```

#### 3.1.B. Terminal 2 (endpoint)

Run the endpoint service. NOTE: for production runs, it will be running on the
target HPC resource (on the head node and/or from the batch job).

```shell
# corresponding virtual environment (e.g., ve_endpoint) should be active,
# env variables RADICAL_BRIDGE_CERT, RADICAL_BRIDGE_URL should be set
orbit-endpoint.py
```

For launching via batch job schedulers, use the wrapper script which sets up the environment:
```shell
orbit-endpoint-wrapper.sh --url wss://bridge.example.org:8000 --name my-hpc-endpoint
```

Example output:
```text
INFO:     [Endpoint] Loaded plugin: lucid
INFO:     [Endpoint] Loaded plugin: xgfabric
INFO:     [Endpoint] Loaded plugin: queue_info
INFO:     [Endpoint] Loaded plugin: sysinfo
INFO:     [Endpoint] Loaded plugin: psij
INFO:     [Endpoint] Loaded plugin: rhapsody
INFO:     Starting ORBIT Service (https://localhost:8000)
INFO:     [Endpoint] Connected to https://localhost:8000

```

The bridge endpoint should confirm the connection coming from the endpoint service
with `[Bridge] Endpoint connected` and `registered connection` messages in the 
terminal 1 related to the bridge.

#### 3.1.C. Terminal 3 (client)

Run a test client.

```shell
# corresponding virtual environment (e.g., ve_endpoint) should be active,
# env variable RADICAL_BRIDGE_URL should be set
#
# get to the directory with examples (within the Endpoint repo)
cd radical.orbit/examples
```

The following example will print out `metrics` using `PluginSysInfo`.
```shell
python3 example_sysinfo.py
```

The following example will try to `submit` a batch job using `PluginPSIJ`.
```shell
python3 example_psij.py
```

Since there is no configured SLURM locally, PSI/J will use the `local` backend.
```text
INFO:     HTTP Request: POST https://localhost:8000/endpoint/list "HTTP/1.1 200 OK"
Using endpoint: <endpoint_hostname>
INFO:     HTTP Request: POST https://localhost:8000/endpoint/list "HTTP/1.1 200 OK"
INFO:     HTTP Request: POST https://localhost:8000/<endpoint_hostname>/psij/register_session "HTTP/1.1 200 OK"
Submitting Job...
INFO:     HTTP Request: POST https://localhost:8000/<endpoint_hostname>/psij/submit/session.51f7dfdc "HTTP/1.1 200 OK"
.....
```

### 3.2. Containerized

All endpoints run within different Docker containers. We use `dev` tag 
for the latest, but yet unstable, configuration for the ORBIT Image.

```shell
export RADICAL_ORBIT_IMAGE=radicalcybertools/radical.orbit
export RADICAL_ORBIT_TAG=dev
# for the demo we use the current `devel` branch
export RADICAL_ORBIT_BRANCH="devel"

# for the demo we use the hostname for the bridge as `bridge`
export RADICAL_BRIDGE_HOSTNAME=bridge
```

```shell
cd radical.orbit/examples/docker
docker build --build-arg GENERATE_BRIDGE_CERT=true \
             --build-arg BRIDGE_IP=127.0.0.1 \
             --build-arg BRIDGE_HOSTNAME=${RADICAL_BRIDGE_HOSTNAME} \
             --build-arg RADICAL_ORBIT_BRANCH=${RADICAL_ORBIT_BRANCH} \
             -t ${RADICAL_ORBIT_IMAGE}:${RADICAL_ORBIT_TAG} .
```

```shell
# start the bridge, endpoint, and client containers in the background
docker compose up -d

# get into the client container and run the example
docker exec -it orbit-client bash

cd /app/radical.orbit/examples
python3 example_sysinfo.py

# docker compose logs -f bridge -f endpoint
# stop and remove containers
#    docker compose down
```

### 3.3. Remote run

All endpoints run on different machines. We will use the RADICAL3 machine 
for the bridge (might be used by multiple endpoint services) and ALCF Polaris 
for the endpoint service, and the local machine for the client.

- **Bridge on RADICAL3**
  - Generate the certificate (should be distributed to the endpoint service and the 
    client) and the key, set the environment (including env variables for cert 
    and key), run the bridge endpoint;
- **Endpoint on ALCF Polaris**
  - Obtain the bridge certificate, set the environment (including env variables for 
    cert and bridge url), run the endpoint service;
    - NOTE: might require to add bridge ip to `no_proxy` env variable (`export no_proxy="<bridge_ip>,$no_proxy"`);
- **Client on local machine**
  - Obtain the bridge certificate, set the environment (including env variables for 
    cert and bridge url), run the client.

