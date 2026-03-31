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

3. Run the recorder script with nsys profile:

```
nsys profile --trace=cuda,nvtx --cudabacktrace=all --duration=60 python3 /app/vsword_recorder.py --config /config/survey.yaml --ram_ringbuffer_path . --output_path /data/ringbuffer
```
