# SPDX-FileCopyrightText: Copyright (c) 2025 Massachusetts Institute of Technology
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

############################################################
# Base image
############################################################
ARG BASE_IMAGE=ghcr.io/ryanvolz/holohub/rf-array-cuda-12-6:edge
FROM ${BASE_IMAGE} AS base

# Set up environment variables (possibly customized by container user)
ENV CUPY_GPU_MEMORY_LIMIT=10%
ENV HOLOSCAN_EXECUTOR_LOG_LEVEL=WARN
ENV HOLOSCAN_LOG_LEVEL=INFO
ENV HOLOSCAN_LOG_FORMAT=DEFAULT

# Set up environment variables (should not be customized by container user)
ENV CUPY_CUDA_ARRAY_INTERFACE_SYNC=0
ENV HOLOSCAN_CUDA_ARRAY_INTERFACE_SYNC=0

# Install any utils needed for execution
ARG DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    iftop \
    iproute2 \
    iputils-ping \
    net-tools \
    python3-pip

# Install Python dependencies not covered by deb packages
RUN python3 -m pip install --no-cache-dir aiomqtt anyio digital-rf exceptiongroup jsonargparse[ruamel,signatures] matplotlib msgspec paho-mqtt

# Users can optionally mount a volume to /app containing
# these scripts, but copy them into the image anyway to allow
# use of these static files without the mounting
COPY --chmod=775 src/recorder_service.py /app/recorder_service.py
COPY --chmod=775 src/spectrogram.py /app/spectrogram.py

############################################################
# MEP image
############################################################
FROM base AS mep
LABEL org.opencontainers.image.description="Holoscan MEP recorder"

# Install nsight-systems for profiling
ARG DEBIAN_FRONTEND=noninteractive
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    nsight-systems

# Set capabilities on python binary needed by the recorder script for realtime
RUN setcap cap_sys_nice+ep $(realpath $(which python3))

# Copy scripts specific to this image
COPY --chmod=777 mep/mep_recorder.py /app/mep_recorder.py

# Set up environment variable defaults for this image
ENV RECORDER_CONFIG_PATH=/config
ENV RECORDER_OUTPUT_PATH=/data/ringbuffer
ENV RECORDER_RAM_RINGBUFFER_PATH=/ramdisk
ENV RECORDER_SCRIPT_PATH=/app/mep_recorder.py
ENV RECORDER_START_CONFIG=default
ENV RECORDER_TMP_RINGBUFFER_PATH=/data/tmp-ringbuffer

ENV HOME=/ramdisk
WORKDIR /ramdisk
ENTRYPOINT ["python3", "/app/recorder_service.py"]

############################################################
# VSWORD image
############################################################
FROM base AS vsword
LABEL org.opencontainers.image.description="Holoscan VSWORD recorder"

# Set capabilities on python binary needed by the recorder script for realtime
# and DPDK:
# (https://doc.dpdk.org/guides-24.11/platform/mlx5.html, running as non-root)
RUN setcap cap_net_admin,cap_net_raw,cap_dac_override,cap_dac_read_search,cap_ipc_lock,cap_sys_admin,cap_sys_nice,cap_sys_rawio+ep $(realpath $(which python3))

# Copy scripts specific to this image
COPY --chmod=777 vsword/vsword_recorder.py /app/vsword_recorder.py

# Set up environment variable defaults for this image
ENV RECORDER_CONFIG_PATH=/config
ENV RECORDER_OUTPUT_PATH=/data/ringbuffer
ENV RECORDER_RAM_RINGBUFFER_PATH=/ramdisk
ENV RECORDER_SCRIPT_PATH=/app/vsword_recorder.py
ENV RECORDER_START_CONFIG=default
ENV RECORDER_TMP_RINGBUFFER_PATH=/data/tmp-ringbuffer

ENV HOME=/ramdisk
WORKDIR /ramdisk
ENTRYPOINT ["python3", "/app/recorder_service.py"]

############################################################
# Default target
############################################################
FROM mep AS default
