#!/bin/bash

DOCKER_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )"
CONTEXT_DIR="$( dirname "$DOCKER_DIR" )"

IMAGE_NAME="${RADICAL_ORBIT_IMAGE:-radicalcybertools/radical.orbit}"
IMAGE_TAG="${RADICAL_ORBIT_TAG:-latest}"
BRANCH="${RADICAL_ORBIT_BRANCH:-master}"
GENERATE_BRIDGE_CERT=true
BRIDGE_IP=127.0.0.1
BRIDGE_HOSTNAME=${RADICAL_BRIDGE_HOSTNAME:-bridge}
PLATFORM="linux/amd64"  # linux/amd64,linux/arm64
NO_CACHE=""

while getopts ":t:b:p:c" option; do
   case $option in
      t) # image tag
         IMAGE_TAG=$OPTARG;;
      b) # radical.orbit branch
         BRANCH=$OPTARG;;
      p) # build platform (e.g., linux/amd64)
         PLATFORM=$OPTARG;;
      c) # no cache
         NO_CACHE="--no-cache";;
     \?) # unknown option
         echo "Unknown option $OPTARG"
         exit 1;;
   esac
done

FULL_TAG="$IMAGE_NAME:$IMAGE_TAG"

echo "Building Docker container: $FULL_TAG ($PLATFORM)"
echo "Dockerfile: $DOCKER_DIR/Dockerfile"
echo "radical.orbit branch: $BRANCH"

docker build $NO_CACHE --platform $PLATFORM \
             --build-arg GENERATE_BRIDGE_CERT="$GENERATE_BRIDGE_CERT" \
             --build-arg BRIDGE_IP="$BRIDGE_IP" \
             --build-arg BRIDGE_HOSTNAME="$BRIDGE_HOSTNAME" \
             --build-arg RADICAL_ORBIT_BRANCH="$BRANCH" \
             -t "$FULL_TAG" -f "$DOCKER_DIR/Dockerfile" "$CONTEXT_DIR"
