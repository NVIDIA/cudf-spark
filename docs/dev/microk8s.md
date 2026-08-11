---
layout: page
title: Setting up a Microk8s Environment
nav_order: 6
parent: Developer Overview
---

# Setting up a Microk8s environment

[Microk8s](https://microk8s.io/) is a lightweight Kubernetes distribution that is suitable for setting up a local
environment for development and testing purposes. These instructions explain how to set up Microk8s on Ubuntu.

## Install Microk8s

It is important to install the 1.20 version because there are some issues in the 1.21 release with GPU support. These
may have been resolved in the 1.22 release but this has not been tested yet with the NVIDIA cuDF
plugin for Apache Spark.

```bash
sudo snap install microk8s --classic --channel=1.20/stable
```

## Permissions
To avoid the need to use `sudo` when running `microk8s` commands, add the current user to the `microk8s` group and
ensure that the user has access to files in the `~/.kube` folder.

```bash
sudo usermod -a -G microk8s $USER
sudo chown -f -R $USER ~/.kube
```

## Generate Kube config

Backup any existing Kube configuration file and then generate a new kube config.

```bash
mkdir -p ~/.kube
microk8s config > ~/.kube/config
```

## Enable DNS and GPU support

```bash
microk8s.enable dns
microk8s.enable gpu
```

## Add cluster host name to /etc/hosts

Add `kubernetes.default.svc.cluster.local` to `/etc/hosts` to map to the same IP address as the host name. This is
usually `127.0.1.1` or `127.0.0.1`.

This host name should be used in the `spark-submit` command by specifying
`--master k8s://https://kubernetes.default.svc.cluster.local:16443`.

## Create Spark service account

```bash
microk8s.kubectl create serviceaccount spark
```

This service account can then be specified in the `spark-submit` command with
`--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark`.

## Specifying a certificate

The Microk8s certificate can be specified with
`--conf spark.kubernetes.authenticate.caCertFile=/var/snap/microk8s/current/certs/ca.crt`.

## set K8S_SECRET env var with token

View a list of secrets using the following command.

```bash
microk8s.kubectl -n kube-system get secrets
```

Look for a secret with a name starting with `default-token-` and run the following command to view the secret.

```bash
microk8s.kubectl -n kube-system describe secret default-token-5bmbh
```

Copy the value of the `token` attribute and use it to set a `K8S_TOKEN` environment variable.

```bash
export K8S_TOKEN=<value from above>
```

This token can then be specified in the `spark-submit` command with
`--conf spark.kubernetes.authenticate.submission.oauthToken=$K8S_TOKEN`

## Building and exporting Docker images

Follow the instructions in [Getting Started with the NVIDIA cuDF plugin for Apache Spark and Kubernetes](https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/kubernetes.html)
to create Docker images containing Spark and the cuDF plugin.

Note that an additional step is required to export the Docker images from the host and import them into the Microk8s
cluster.

For example, the following commands can be used to export a Docker image named `cudf-spark` and import it into
Microk8s.

```bash
docker save cudf-spark > /tmp/cudf-spark.tar
microk8s ctr image import /tmp/cudf-spark.tar
```

## Spark Submit

After completing the above steps it should now be possible to use `spark-submit` to run a Spark job in Microk8s. The
following partial `spark-submit` example shows the settings that should be present.

```
$SPARK_HOME/bin/spark-submit \
    --master k8s://https://kubernetes.default.svc.cluster.local:16443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.authenticate.caCertFile=/var/snap/microk8s/current/certs/ca.crt \
    --conf spark.kubernetes.authenticate.submission.oauthToken=$K8S_TOKEN \
    --conf spark.kubernetes.container.image=cudf-spark \
```

## Further Reading

The following documentation pages and blog posts provide more information on setting up Microk8s,
Apache Spark, and the cuDF plugin.

- [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Setting up Apache Spark on Kubernetes with microk8s](https://www.waitingforcode.com/apache-spark/setting-up-apache-spark-kubernetes-microk8s/read)
- [Accelerated single-node Kubernetes with microk8s, Kubeflow, and RAPIDS.ai](https://chapeau.freevariable.com/2021/04/microk8s.html)
