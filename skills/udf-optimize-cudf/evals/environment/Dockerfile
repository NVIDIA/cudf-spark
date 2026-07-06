# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

###
#
# Pre-merge CI image for the UDF skills integration tests and evals.
# This is a mirror of `skills/udf-convert-to-cuda/templates/cuda/Dockerfile`,
# with the additions of python3 + pip to run integration tests.
#
###

# NOTE: skills CI currently requires the first command to be `FROM`,
# hence why we do not parametrize the image versions using `ARG=`.

# rockylinux8 is used in accordance with https://github.com/NVIDIA/cudf-spark-jni/blob/main/ci/Dockerfile
FROM nvidia/cuda:12.9.1-devel-rockylinux8

ARG TOOLSET_VERSION=14
ARG CMAKE_VERSION=4.0.3
ARG CMAKE_ARCH=x86_64
ARG CCACHE_VERSION=4.11.2
ARG MAVEN_VERSION=3.9.9
ARG MAVEN_BASE_URL=https://archive.apache.org/dist/maven/maven-3
ARG PARALLEL_LEVEL=10
ARG PYTHON_VERSION=3.11

ENV TOOLSET_VERSION=${TOOLSET_VERSION}
ENV PARALLEL_LEVEL=${PARALLEL_LEVEL}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN dnf --enablerepo=powertools install -y \
  gcc-toolset-${TOOLSET_VERSION} \
  git \
  java-17-openjdk-devel \
  ninja-build \
  patch \
  python${PYTHON_VERSION} \
  python${PYTHON_VERSION}-pip \
  scl-utils \
  tar \
  unzip \
  wget \
  zlib-devel && \
  alternatives --install /usr/bin/python3 python3 /usr/bin/python${PYTHON_VERSION} 100 && \
  alternatives --set python3 /usr/bin/python${PYTHON_VERSION} && \
  ln -sf /usr/bin/python${PYTHON_VERSION} /usr/bin/python && \
  ln -sf /usr/bin/pip${PYTHON_VERSION} /usr/bin/pip && \
  dnf clean all

RUN cd /usr/local && \
  wget --quiet https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  tar zxf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz && \
  rm cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz

# The rockylinux8 maven package is too old for the template build plugins.
# Install a modern Maven binary on PATH instead.
RUN cd /usr/local && \
  wget --quiet ${MAVEN_BASE_URL}/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
  tar zxf apache-maven-${MAVEN_VERSION}-bin.tar.gz && \
  rm apache-maven-${MAVEN_VERSION}-bin.tar.gz

# The CUDA image activates gcc-toolset SCL via a `bash -l ` entrypoint plus `/etc/profile.d` scripts
# sourced on every container start via a LOGIN shell. However, Harbor evals run with a non-login shell
# (`sh -c ...`), so instead, this image bakes the environment into ENV and does not use an ENTRYPOINT.
ENV PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/bin:${JAVA_HOME}/bin:/usr/local/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}/bin:/usr/local/apache-maven-${MAVEN_VERSION}/bin:${PATH}
ENV LD_LIBRARY_PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib64:/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
ENV PKG_CONFIG_PATH=/opt/rh/gcc-toolset-${TOOLSET_VERSION}/root/usr/lib64/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}

# ccache for interactive builds
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
