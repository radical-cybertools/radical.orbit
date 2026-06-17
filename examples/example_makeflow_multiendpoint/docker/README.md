# Dockerized Multi-Endpoint Makeflow Example

This directory contains the Docker configuration to run the [Multi-Endpoint Makeflow example](../README.md) in a fully containerized environment. This setup simulates a distributed workflow spanning multiple endpoint nodes using Docker containers.

## Infrastructure Overview

The `docker-compose.yaml` file defines the following services:
- **bridge**: The central coordination point for the workflow.
- **endpoint-a**: A radical.orbit instance named `endpoint_a`.
- **endpoint-b**: A radical.orbit instance named `endpoint_b`.
- **client**: A container for launching the Makeflow workflow.

The build context is set to the parent directory (`..`), so `pools.json` and `workflow.makeflow` are automatically included in the image during build.

## Running the Example

> [!WARNING]
> TEMPORARY ENVIRONMENT VARIABLES FOR THE DEMO

```shell
export RADICAL_ORBIT_TAG=makeflow
export RADICAL_ORBIT_BRANCH=feature/task-dispatcher
```

1. **Start the containers:**
   Build and start the infrastructure in detached mode.

   ```bash
   docker compose up -d
   ```

   *Note: On the first run, this will build the images. Alternatively, you can build them manually using the `./build.sh` script (see [Building the Image](#building-the-image) below).*

2. **Wait for services to be ready:**
   The endpoint services depend on the bridge being healthy. You can monitor the startup process with:

   ```bash
   docker compose logs -f
   ```
   Wait until you see messages indicating that `endpoint_a` and `endpoint_b` have successfully connected to the bridge. Press `Ctrl+C` to stop following logs.

3. **Run the workflow:**
   Execute the Makeflow workflow from the client container. Since the workflow file is copied into the image, you can run it directly:

   ```bash
   docker exec -it radical-orbit-client radical-orbit-makeflow workflow.makeflow
   ```

   The `radical-orbit-makeflow` script will:
   - Preprocess the workflow.
   - Dispatch tasks to the appropriate endpoint nodes (`endpoint_a` or `endpoint_b`).
   - Manage cross-endpoint file transfers through the client.

4. **Verify the output:**
   After the workflow completes, check for the generated `summary.txt`:

   ```bash
   docker exec radical-orbit-client cat summary.txt
   ```

## Cleanup

To stop and remove the containers, networks, and volumes created by this example:

```bash
docker compose down
```

---

### Advanced Configuration

- **Development:** To use a specific branch of `radical.orbit`, set the `RADICAL_ORBIT_BRANCH` environment variable:
  ```bash
  RADICAL_ORBIT_BRANCH=feature-branch docker compose build
  ```
- **Image Overrides:** You can use a custom image by setting `RADICAL_ORBIT_IMAGE` and `RADICAL_ORBIT_TAG`.

### Building the Image

For more control over the build process (e.g., specifying a different branch or target platform), use the provided `build.sh` script:

```bash
./build.sh -b master -t latest -p linux/amd64
```

**Available Options:**
- `-b <branch>`: The `radical.orbit` branch to clone and install (default: `master`).
- `-t <tag>`: The tag for the resulting image (default: `latest`).
- `-p <platform>`: The target build platform, e.g., `linux/amd64` or `linux/arm64` (default: `linux/amd64`).
- `-c`: Build without cache (`--no-cache`).

The script also passes bridge configuration arguments (`GENERATE_BRIDGE_CERT`, `BRIDGE_IP`, `BRIDGE_HOSTNAME`) to the Docker build process.
