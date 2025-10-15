import dataclasses
import json
import os
import pathlib
import shutil
import signal
import socket
import time
import traceback
from typing import Any, Optional

import aiomqtt
import anyio
import exceptiongroup
import jsonargparse
from ruamel.yaml import YAML


@dataclasses.dataclass(kw_only=True)
class RecorderService:
    # service configuration variables
    announce_topic: str = "announce/{service.name}"
    command_topic: str = "{service.name}/command"
    config_path: os.PathLike = "config"
    name: str = "recorder"
    node_id: Optional[str] = None
    output_path: os.PathLike = "/data/ringbuffer"
    ram_ringbuffer_path: os.PathLike = "."
    script_path: os.PathLike = "recorder.py"
    start_config: str = "default"
    status_topic: str = "{service.name}/status"
    tmp_ringbuffer_path: os.PathLike = "/data/tmp-ringbuffer"
    # service state variables
    config: dict[str, Any] = dataclasses.field(default_factory=dict, init=False)
    loadable_configs: dict[str, dict[str, Any]] = dataclasses.field(
        default_factory=dict,
        init=False,
    )
    recording_enabled: bool = dataclasses.field(default=False, init=False)
    recording_scope: Optional[anyio.CancelScope] = dataclasses.field(
        default=None, init=False
    )

    def __post_init__(self):
        if self.node_id is None:
            self.node_id = os.getenv("NODE_ID", socket.gethostname())
        self.config_path = pathlib.Path(self.config_path)
        self.output_path = pathlib.Path(self.output_path)
        self.ram_ringbuffer_path = pathlib.Path(self.ram_ringbuffer_path)
        self.script_path = pathlib.Path(self.script_path)
        self.tmp_ringbuffer_path = pathlib.Path(self.tmp_ringbuffer_path)

        self.announce_topic = self.announce_topic.format(service=self)
        self.command_topic = self.command_topic.format(service=self)
        self.status_topic = self.status_topic.format(service=self)


def load_configs(service):
    config_paths = sorted(service.config_path.glob("*.yaml"))
    yaml = YAML(typ="safe")
    for p in config_paths:
        service.loadable_configs[p.stem] = jsonargparse.dict_to_namespace(yaml.load(p))
    if service.start_config in service.loadable_configs:
        service.config = service.loadable_configs[service.start_config]
    else:
        service.config = list(service.loadable_configs.keys())[0]


async def send_announce(client, service):
    payload = {
        "title": "Recorder",
        "description": f"Record data to {str(service.output_path)}",
        "author": "Ryan Volz <rvolz@mit.edu>",
        "url": "ghcr.io/ryanvolz/holoscan_recorder/mep:latest",
        "source": "https://github.com/ryanvolz/holoscan_recorder",
        "output": {
            "output_name": {"type": "disk", "value": f"{str(service.output_path)}"},
        },
        "version": "0.2",
        "type": "service",
        "time_started": time.time(),
    }
    await client.publish(service.announce_topic, json.dumps(payload), retain=True)


async def send_status(client, service):
    payload = {
        "state": "recording" if service.recording_enabled else "waiting",
        "timestamp": time.time(),
    }
    await client.publish(service.status_topic, json.dumps(payload), retain=True)


async def send_error(client, service, message, response_topic=None):
    if response_topic is None:
        response_topic = f"{service.name}/error"
    payload = {
        "message": message,
        "timestamp": time.time(),
    }
    await client.publish(response_topic, json.dumps(payload))


async def send_config(client, service, value, response_topic=None):
    if response_topic is None:
        response_topic = f"{service.name}/config/response"

    payload = {
        "value": value,
        "timestamp": time.time(),
    }
    await client.publish(response_topic, json.dumps(payload))


async def run_drf_mirror(service):
    command = [
        "drf",
        "mirror",
        "cp",
        str(service.ram_ringbuffer_path),
        str(service.output_path),
    ]
    await anyio.run_process(command, stdout=None, stderr=None, check=False)


async def run_drf_ringbuffer(service):
    command = [
        "drf",
        "ringbuffer",
        "-l",
        "1",
        "--status_interval",
        "10",
        str(service.ram_ringbuffer_path),
    ]
    try:
        await anyio.run_process(command, stdout=None, stderr=None, check=False)
    finally:
        shutil.rmtree(service.ram_ringbuffer_path, ignore_errors=True)


async def run_drf_mirror_tmp(service):
    shutil.rmtree(service.tmp_ringbuffer_path, ignore_errors=True)
    command = [
        "drf",
        "mirror",
        "cp",
        "--link",
        str(service.output_path),
        str(service.tmp_ringbuffer_path),
    ]
    await anyio.run_process(command, stdout=None, stderr=None, check=False)


async def run_drf_ringbuffer_tmp(service):
    command = [
        "drf",
        "ringbuffer",
        "-l",
        "1",
        "--status_interval",
        "31536000",
        str(service.tmp_ringbuffer_path),
    ]
    try:
        await anyio.run_process(command, stdout=None, stderr=None, check=False)
    finally:
        shutil.rmtree(service.tmp_ringbuffer_path, ignore_errors=True)


async def run_recorder(client, service):
    command = [
        "python3",
        str(service.script_path),
        "--config",
        json.dumps(service.config.as_dict()),
        "--ram_ringbuffer_path",
        str(service.ram_ringbuffer_path),
        "--output_path",
        str(service.output_path),
    ]
    with anyio.CancelScope() as scope:
        service.recording_scope = scope
        await send_status(client, service)
        async with await anyio.open_process(
            command, stdout=None, stderr=None
        ) as process:
            try:
                await process.wait()
            finally:
                if process.returncode is None:
                    # process is still running, stop it gracefully
                    process.send_signal(signal.SIGINT)
                # clean up ram ringbuffer directory
                for item in service.ram_ringbuffer_path.rglob("*"):
                    try:
                        if item.is_dir():
                            shutil.rmtree(item, ignore_errors=True)
                        else:
                            item.unlink(missing_ok=True)
                    except OSError:
                        pass
                service.recording_enabled = False
                service.recording_scope = None
                with anyio.CancelScope(shield=True):
                    await process.wait()
                    await send_status(client, service)


def disable_recording(service):
    if service.recording_enabled:
        service.recording_enabled = False
        service.recording_scope.cancel()
        service.recording_scope = None


def enable_recording(client, service, task_group):
    if not service.recording_enabled:
        service.recording_enabled = True
        task_group.start_soon(run_recorder, client, service)


async def process_config_command(client, service, payload):
    cmd = payload["task_name"].removeprefix("config.")
    args = payload.get("arguments", {})
    response_topic = payload.get("response_topic", None)
    try:
        if cmd == "get":
            key = args.get("key", "")
            try:
                if not key:
                    value = service.config
                else:
                    value = service.config[key]
                if isinstance(value, jsonargparse.Namespace):
                    value = value.as_dict()
            except KeyError:
                msg = f"ERROR config.get: key '{key}' not found."
                await send_error(client, service, msg, response_topic)
            else:
                await send_config(client, service, value, response_topic)
        if cmd == "set":
            key = args.get("key", "")
            val = args["value"]
            if isinstance(val, dict):
                val = jsonargparse.dict_to_namespace(val)
            service.config.update(val, key)
            await send_config(client, service, service.config.as_dict(), response_topic)
        if cmd == "list":
            available_config_names = sorted(list(service.loadable_configs.keys()))
            if response_topic is None:
                response_topic = f"{service.name}/config/response"
            payload = {
                "available_configs": available_config_names,
                "timestamp": time.time(),
            }
            await client.publish(response_topic, json.dumps(payload))
        if cmd == "load":
            config_name = args["name"]
            try:
                service.config = service.loadable_configs[config_name]
            except KeyError:
                msg = f"ERROR config.load: configuration '{config_name}' not found."
                await send_error(client, service, msg, response_topic)
            else:
                await send_config(
                    client, service, service.config.as_dict(), response_topic
                )
    except Exception:
        msg = f"ERROR config:\n{traceback.format_exc()}"
        await send_error(client, service, msg, response_topic)


async def process_commands(client, service, task_group):
    async for message in client.messages:
        payload = json.loads(message.payload.decode())
        if payload["task_name"] == "disable":
            disable_recording(service)
        if payload["task_name"] == "enable":
            enable_recording(client, service, task_group)
        if payload["task_name"] == "status":
            await send_status(client, service)
        if payload["task_name"].startswith("config."):
            await process_config_command(client, service, payload)


async def main(service):
    load_configs(service)
    will = aiomqtt.Will(
        f"{service.name}/status",
        payload=json.dumps({"state": "offline"}),
        qos=0,
        retain=True,
    )
    client = aiomqtt.Client(
        "localhost",
        1883,
        keepalive=60,
        will=will,
    )
    interval = 5  # seconds
    while True:
        try:
            async with client:
                await client.subscribe(service.command_topic)
                await send_announce(client, service)
                await send_status(client, service)
                with exceptiongroup.catch(
                    {
                        Exception: lambda exc: traceback.print_exc(),
                    }
                ):
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(run_drf_mirror, service)
                        tg.start_soon(run_drf_ringbuffer, service)
                        tg.start_soon(run_drf_mirror_tmp, service)
                        tg.start_soon(run_drf_ringbuffer_tmp, service)
                        tg.start_soon(process_commands, client, service, tg)
        except aiomqtt.MqttError:
            msg = (
                "Connection to MQTT server lost;"
                f" Reconnecting in {interval} seconds ..."
            )
            print(msg)
            await anyio.sleep(interval)


if __name__ == "__main__":
    service = jsonargparse.auto_cli(
        RecorderService, env_prefix="RECORDER", default_env=True
    )
    anyio.run(main, service)
