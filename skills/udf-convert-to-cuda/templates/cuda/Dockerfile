# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Reproducible build image for native CUDA RapidsUDF code.
ARG CUDA_VERSION=12.9.1
ARG LINUX_VERSION=rockylinux8

FROM nvidia/cuda:${CUDA_VERSION}-devel-${LINUX_VERSION}

ARG TOOLSET_VERSION=14
ARG CMAKE_VERSION=3.30.4
ARG CMAKE_ARCH=x86_64
ARG CCACHE_VERSION=4.11.2
ARG PARALLEL_LEVEL=10

ENV TOOLSET_VERSION=${TOOLSET_VERSION}
ENV PARALLEL_LEVEL=${PARALLEL_LEVEL}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN dnf --enablerepo=powertools install -y \
  gcc-toolset-${TOOLSET_VERSION} \
  git \
  java-17-openjdk-devel \
  maven \
  ninja-build \
  patch \
  python39 \
  scl-utils \
  tar \
  unzip \
  wget \
  zlib-devel \
  && alternatives --set python /usr/bin/python3

RUN cd /usr/local && \
  wget --quiet https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  tar zxf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  rm cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz
ENV PATH=${JAVA_HOME}/bin:/usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin:${PATH}

# Bake the SCL activation and Java 17 environment into /etc/profile.d so they are restored by `bash -l` on every container start.
RUN printf 'source /opt/rh/gcc-toolset-%s/enable\n' "${TOOLSET_VERSION}" \
      > /etc/profile.d/scl-gcc-toolset.sh && \
    printf '%s\n%s\n' \
      'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk' \
      'export PATH=$JAVA_HOME/bin:$PATH' \
      > /etc/profile.d/java17.sh

RUN cd /tmp && \
  wget --quiet https://github.com/ccache/ccache/releases/download/v${CCACHE_VERSION}/ccache-${CCACHE_VERSION}.tar.gz && \
  tar zxf ccache-${CCACHE_VERSION}.tar.gz && \
  rm ccache-${CCACHE_VERSION}.tar.gz && \
  cd ccache-${CCACHE_VERSION} && \
  mkdir build && \
  cd build && \
  scl enable gcc-toolset-${TOOLSET_VERSION} \
    "cmake .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DZSTD_FROM_INTERNET=ON \
      -DREDIS_STORAGE_BACKEND=OFF && \
    cmake --build . --parallel ${PARALLEL_LEVEL} --target install" && \
  cd ../.. && \
  rm -rf ccache-${CCACHE_VERSION}

ENTRYPOINT ["bash", "-l"]
