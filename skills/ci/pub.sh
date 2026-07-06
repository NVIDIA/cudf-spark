#!/bin/bash

docker build \
    -t artifactory.nvidia.com/sw-spark-docker/spark-rapids-skills:rocky8-cuda12.9.1-python3.11-j17 \
    -f Dockerfile.pre-merge .
    --build-arg MAVEN_BASE_URL=<internal maven-3 mirror>

# # Afterwards, run:
#
# docker login artifactory.nvidia.com
# docker push artifactory.nvidia.com/sw-spark-docker/spark-rapids-skills:rocky8-cuda12.9.1-python3.11-j17
