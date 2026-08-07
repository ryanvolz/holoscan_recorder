import dataclasses
import functools
import itertools
import logging
import operator
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
import msgspec
from ruamel.yaml import YAML

logger = logging.getLogger("recorder_service")
logger.setLevel(os.environ.get("RECORDER_SERVICE_LOG_LEVEL", "INFO"))
logger.propagate = False
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.DEBUG)
logger.addHandler(_console_handler)


def deep_update(mapping: dict, *updating_mappings: dict) -> dict:
    """Update nested dictionary from another nested dictionary"""
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if (
                k in updated_mapping
                and isinstance(updated_mapping[k], dict)
                and isinstance(v, dict)
            ):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping


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
    response_topic: str = "{service.name}/response"
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
        self.response_topic = self.response_topic.format(service=self)
        self.status_topic = self.status_topic.format(service=self)


def load_configs(service):
    config_paths = sorted(service.config_path.glob("*.yaml"))
    yaml = YAML(typ="safe")
    for p in config_paths:
        service.loadable_configs[p.stem] = yaml.load(p)
    if service.start_config in service.loadable_configs:
        service.cfg = service.loadable_configs[service.start_config]
    else:
        service.cfg = list(service.loadable_configs.keys())[0]


async def send_announce(client, service):
    payload = {
        "title": "Recorder",
        "description": f"Record data to {str(service.output_path)}",
        "author": "Ryan Volz <rvolz@mit.edu>",
        "url": "ghcr.io/ryanvolz/holoscan_recorder/mep",
        "source": "https://github.com/ryanvolz/holoscan_recorder",
        "output": {
            "rf_data": {"type": "disk", "value": f"{str(service.output_path)}"},
            "status": {
                "type": "mqtt",
                "value": f"{service.status_topic}",
            },
        },
        "version": "0.3",
        "type": "service",
        "time_started": time.time(),
        "command_topic": f"{service.command_topic}",
        "default_response_topic": f"{service.response_topic}",
        "commands": {
            "disable": {
                "task_name": "disable",
                "arguments": {},
            },
            "enable": {
                "task_name": "enable",
                "arguments": {},
            },
            "status": {
                "task_name": "status",
                "arguments": {},
            },
            "config.get": {
                "task_name": "config.get",
                "arguments": {"key": "str"},
            },
            "config.set": {
                "task_name": "config.set",
                "arguments": {"key": "str", "value": "Any"},
            },
            "config.list": {
                "task_name": "config.list",
                "arguments": {},
            },
            "config.load": {
                "task_name": "config.load",
                "arguments": {"name": "str"},
            },
        },
    }
    json_payload = msgspec.json.encode(payload)
    logger.debug(
        f"Announcing {service.name} on {service.announce_topic}:\n{json_payload}"
    )
    await client.publish(service.announce_topic, json_payload, retain=True)


async def send_status(client, service):
    payload = {
        "output_path": f"{str(service.output_path)}",
        "state": "recording" if service.recording_enabled else "waiting",
        "timestamp": time.time(),
    }
    json_payload = msgspec.json.encode(payload)
    logger.debug(
        f"Sending {service.name} status to {service.status_topic}:\n{json_payload}"
    )
    await client.publish(service.status_topic, json_payload, retain=True)
    return payload


async def send_response(client, service, response, command_payload=None):
    if command_payload is None:
        command_payload = {}
    response_topic = command_payload.get("response_topic", service.response_topic)
    session_id = command_payload.get("session_id", None)
    task_name = command_payload.get("task_name", None)

    response.update(
        session_id=session_id,
        task_name=task_name,
        timestamp=time.time(),
    )
    json_payload = msgspec.json.encode(response)
    logger.debug(
        f"Sending {service.name} response to {response_topic}:\n{json_payload}"
    )
    await client.publish(response_topic, json_payload)
    return response


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
        "31536000",
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
        msgspec.json.encode(service.cfg),
        "--ram_ringbuffer_path",
        str(service.ram_ringbuffer_path),
        "--output_path",
        str(service.output_path),
    ]
    with anyio.CancelScope() as scope:
        service.recording_scope = scope
        clear_ringbuffers(service)
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
                clear_ringbuffers(service)
                service.recording_enabled = False
                service.recording_scope = None
                with anyio.CancelScope(shield=True):
                    await process.wait()
                    await send_status(client, service)


def clear_ringbuffers(sevice):
    for item in itertools.chain(
        service.ram_ringbuffer_path.rglob("*"), service.tmp_ringbuffer_path.rglob("*")
    ):
        try:
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        except OSError:
            pass


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
    try:
        if cmd == "get":
            key = args.get("key", "")
            try:
                if not key:
                    value = service.cfg
                else:
                    # does service.cfg.{key} where key can have additional dot levels
                    value = operator.attrgetter(key)(service.cfg)
                if isinstance(value, jsonargparse.Namespace):
                    value = value.as_dict()
            except AttributeError:
                msg = f"Key '{key}' not found."
                logger.warning(msg)
                response = {"exception": msg}
                await send_response(client, service, response, payload)
            else:
                logger.debug(f"Got config key {key}: {value}")
                response = {"value": value}
                await send_response(client, service, response, payload)
        if cmd == "set":
            key = args.get("key", "")
            val = args["value"]
            if not key:
                update_dict = val
            else:
                update_dict = functools.reduce(
                    lambda v, k: {k: v}, reversed(key.split(".")), val
                )
            updated_cfg = deep_update(service.cfg, update_dict)
            service.cfg = updated_cfg
            logger.debug(f"Set config key {key}: {val}")
            response = {"value": service.cfg}
            await send_response(client, service, response, payload)
        if cmd == "list":
            available_config_names = sorted(list(service.loadable_configs.keys()))
            logger.debug(f"Listing available configs: {available_config_names}")
            response = {"available_configs": available_config_names}
            await send_response(client, service, response, payload)
        if cmd == "load":
            config_name = args["name"]
            try:
                service.cfg = service.loadable_configs[config_name]
            except KeyError:
                msg = f"Configuration '{config_name}' not found."
                logger.warning(msg)
                response = {"exception": msg}
                await send_response(client, service, response, payload)
            else:
                logger.debug(f"Loaded config {config_name}")
                response = {"value": service.cfg}
                await send_response(client, service, response, payload)
    except Exception:
        logger.exception(
            f"Error processing config payload:\n{msgspec.json.encode(payload)}"
        )
        response = {"exception": traceback.format_exc()}
        await send_response(client, service, response, payload)


async def process_commands(client, service, task_group):
    logger.info(f"Service {service.name} listening for commands")
    async for message in client.messages:
        payload = msgspec.json.decode(message.payload)
        if payload["task_name"] == "disable":
            logger.info("Stopping recording script")
            disable_recording(service)
        if payload["task_name"] == "enable":
            logger.info("Starting recording script")
            enable_recording(client, service, task_group)
        if payload["task_name"] == "status":
            logger.info("Processing status command")
            status_response = await send_status(client, service)
            await send_response(client, service, status_response, payload)
        if payload["task_name"].startswith("config."):
            await process_config_command(client, service, payload)


async def main(service):
    load_configs(service)
    will = aiomqtt.Will(
        service.status_topic,
        payload=msgspec.json.encode({"state": "offline"}),
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
                        Exception: lambda exc: logger.error("Exception", exc_info=exc),
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
            logger.warning(msg)
            await anyio.sleep(interval)


if __name__ == "__main__":
    logger.info("Starting recorder_service")
    service = jsonargparse.auto_cli(
        RecorderService, env_prefix="RECORDER", default_env=True
    )
    anyio.run(main, service)
