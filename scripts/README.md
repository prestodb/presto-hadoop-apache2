# Hadoop Native Library Generation (Linux)

This guide explains how to generate Hadoop native libraries for Linux using a Dockerfile.
## Prerequisites

- Docker installed on your machine
- Dockerfile present in your working directory

## Steps to Build and Extract Hadoop Native Libraries

### 1. Build Docker Image

Run the following command in the same directory as your Dockerfile:

docker build -t hadoop-native .

This will create a Docker image named hadoop-native

### 2. Run Docker Container

Start a container from the image and open an interactive shell:

docker run -it --name hadoop-native-check hadoop-native bash

This launches a container named hadoop-native-check and gives you access to its shell.

### 3. Locate the Generated Library

Once inside the container, the generated Hadoop native library can be found at:

/opt/hadoop/hadoop-common-project/hadoop-common/target/native/target/usr/local/lib/

The file name will be :
libhadoop.so

Zstandard development libraries file can be found at /usr/lib64/

The file name will be:
libzstd.so

### 4. Copy the Library to Local System

Use the following command to copy the libhadoop.so file from the container to your local machine:

docker cp <container_id>:/opt/hadoop/hadoop-common-project/hadoop-common/target/native/target/usr/local/lib/libhadoop.so <local_path>

Replace:
container_id with your running container's ID or name (e.g., hadoop-native-check)
local_path with your desired local directory
