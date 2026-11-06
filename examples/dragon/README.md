# Dragon DDict endpoint in a container (Orbit-Dragon POC)

An ORBIT endpoint with the `dragon` plugin runs in a container next to a
single-node Dragon runtime (dragonhpc 0.15.0).  A client on the host
creates DDicts and reads and writes them through the ORBIT broker.

The image installs this source tree.  It runs as a single-node runtime
only; multi-node Dragon in containers waits for HPE's launcher.

## Credentials

ORBIT does not generate credentials.  `mkcreds.sh` creates a self-signed
cert/key and an ingress token in `./creds` (git-ignored):

```sh
./mkcreds.sh
```

## Broker and endpoint together

```sh
docker compose up --build
```

The broker listens on host port `$BROKER_PORT` (default 8000).  Run the
client on the host against it:

```sh
export RADICAL_ORBIT_BROKER_URL=https://localhost:8000
export RADICAL_ORBIT_BROKER_CERT=$PWD/creds/broker_cert.pem
export RADICAL_ORBIT_BROKER_TOKEN=$(cat creds/broker.token)
python ../example_dragon_ddict.py dragon-pod
```

## Endpoint only, external broker

The endpoint dials out to a broker elsewhere, as it does on HPC.  Put that
broker's cert and token into `./creds` (`broker_cert.pem`,
`broker.token`), then:

```sh
RADICAL_ORBIT_BROKER_URL=https://broker.example.org:8000 \
    docker compose --profile external up --build endpoint-external
```

## Notes

- Dragon keeps channels and DDict memory in `/dev/shm`; the compose file
  sets `shm_size: 4gb`.  A DDict's `total_mem` must fit there.
- The endpoint starts the Dragon runtime on the first DDict create
  (`RADICAL_ORBIT_DRAGON_RUNTIME=start`), and stops it on exit.
- `--runtime-check` in the example needs the client inside the Dragon
  runtime, so it does not apply to a host-side client.
