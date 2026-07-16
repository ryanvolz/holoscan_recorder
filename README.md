## Profiling

1. Enter a running docker container:

```
docker compose exec -u root recorder bash
```

2. Install Nsight Systems:

```
apt update
apt install nsight-systems
```

3. Re-enter the runner docker container as normal user:

```
docker compose exec recorder bash
```

4. Run the recorder script with nsys profile:

```
HOLOSCAN_ENABLE_PROFILE=1 nsys profile --trace=cuda,nvtx,osrt --cudabacktrace=all --duration=60 python3 /app/vsword_recorder.py --config /config/survey.yaml --ram_ringbuffer_path . --output_path /data/ringbuffer
```

## Analyzing with compute-sanitizer

0. This only seems to work when run as the root user, perhaps even requiring
```
privileged: true
security_opt:
  - seccomp=unconfined
```
for the Docker container.

1. Enter a running docker container as root:

```
docker compose exec -u root recorder bash
```

2. Install cuda-sanitizer:

```
apt update
apt install cuda-sanitizer-12-2
```

3. Run the recorder script with compute-sanitizer:

```
compute-sanitizer --log-file sanitizer.log --tool memcheck --target-processes all python3 /app/vsword_recorder.py --config /config/survey.yaml --ram_ringbuffer_path . --output_path /data/ringbuffer
```

## Debugging with cuda-gdb

See also https://docs.nvidia.com/holoscan/sdk-user-guide/using-the-sdk/debugging#enabling-core-dump.

0. User needs to be in `debug` group and container needs capability `SYS_PTRACE`. See `cap_add` and `group_add` docker compose options.

1. Enter a running docker container:

```
docker compose exec -u root recorder bash
```

2. Install cuda-gdb:

```
apt update
apt install cuda-gdb-12-2
```

3. Re-enter the runner docker container as normal user:

```
docker compose exec recorder bash
```

4. Run the recorder script with CUDA_DEVICE_WAITS_ON_EXCEPTION=1 and attach later:

```
CUDA_DEVICE_WAITS_ON_EXCEPTION=1 python3 /app/vsword_recorder.py --config /config/survey.yaml --ram_ringbuffer_path . --output_path /data/ringbuffer
cuda-gdb --pid={PID}
```
