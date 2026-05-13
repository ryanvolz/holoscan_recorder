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
nsys profile --trace=cuda,nvtx --cudabacktrace=all --duration=60 python3 /app/vsword_recorder.py --config /config/survey.yaml --ram_ringbuffer_path . --output_path /data/ringbuffer
```

## Debugging with cuda-gdb

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
