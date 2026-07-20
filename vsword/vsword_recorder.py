#!/usr/bin/env python3

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

import dataclasses
import logging
import os
import pathlib
import signal
import sys
import tempfile
import typing

import holoscan
import jsonargparse
import matplotlib as mpl
from holohub import rf_array
from holohub.rf_array.digital_metadata import DigitalMetadataSink
from holohub.rf_array.params import (
    DigitalRFSinkParams,
    NetConnectorAdvancedParams,
    ResamplePolyParams,
    RotatorScheduledParams,
    SubchannelSelectParams,
    add_chunk_kwargs,
)
from jsonargparse.typing import NonNegativeInt, PositiveInt

from spectrogram import (
    Spectrogram,
    SpectrogramMQTT,
    SpectrogramMQTTParams,
    SpectrogramOutput,
    SpectrogramOutputParams,
    SpectrogramParams,
)

mpl.use("agg")

# set up Holoscan logger
env_log_level = os.environ.get("HOLOSCAN_LOG_LEVEL", "WARN").upper()
log_level_map = {
    "OFF": "NOTSET",
    "CRITICAL": "CRITICAL",
    "ERROR": "ERROR",
    "WARN": "WARNING",
    "INFO": "INFO",
    "DEBUG": "DEBUG",
    "TRACE": "DEBUG",
}
log_level = log_level_map[env_log_level]
holoscan_handler = logging.StreamHandler()
holoscan_handler.setFormatter(
    logging.Formatter(
        fmt="[{levelname}] [{filename}:{lineno}] {message}",
        style="{",
    )
)
holoscan_handler.setLevel(log_level)
holoscan_logger = logging.getLogger("holoscan")
holoscan_logger.setLevel(log_level)
holoscan_logger.propagate = False
holoscan_logger.addHandler(holoscan_handler)

jsonargparse.set_parsing_settings(docstring_parse_attribute_docstrings=True)


@dataclasses.dataclass
class SchedulerParams:
    """Event-based scheduler parameters"""

    worker_thread_number: PositiveInt = 2
    """Number of worker threads"""
    stop_on_deadlock: bool = True
    """Whether the application will terminate if a deadlock occurs"""
    stop_on_deadlock_timeout: int = 500
    """Time (in ms) to wait before determining that a deadlock has occurred"""


@dataclasses.dataclass
class PipelineParams:
    """Pipeline configuration parameters"""

    selector0: bool = True
    "Enable / disable subchannel selector"
    converter0: bool = True
    "Enable / disable complex int to float converter"
    rotator0: bool = True
    "Enable / disable frequency rotator"
    resampler0: bool = True
    "Enable / disable the first stage resampler"
    digital_rf0: bool = True
    "Enable / disable writing output to Digital RF"
    metadata0: bool = True
    "Enable / disable writing inherent and user-supplied Digital RF metadata"
    spectrogram0: bool = True
    "Enable / disable spectrogram processing"
    spectrogram_mqtt0: bool = True
    "Enable / disable spectrogram output over MQTT"
    spectrogram_output0: bool = True
    "Enable / disable spectrogram output to files"
    selector1: bool = True
    "Enable / disable subchannel selector"
    converter1: bool = True
    "Enable / disable complex int to float converter"
    rotator1: bool = True
    "Enable / disable frequency rotator"
    resampler1: bool = True
    "Enable / disable the first stage resampler"
    digital_rf1: bool = True
    "Enable / disable writing output to Digital RF"
    metadata1: bool = True
    "Enable / disable writing inherent and user-supplied Digital RF metadata"
    spectrogram1: bool = True
    "Enable / disable spectrogram processing"
    spectrogram_mqtt1: bool = True
    "Enable / disable spectrogram output over MQTT"
    spectrogram_output1: bool = True
    "Enable / disable spectrogram output to files"


@dataclasses.dataclass
class BasicNetworkOperatorParams:
    """Basic network operator parameters"""

    ip_addr: str = "0.0.0.0"
    """IP address to bind to"""
    dst_port: NonNegativeInt = 60000
    "UDP or TCP port to listen on"
    l4_proto: str = "udp"
    "Layer 4 protocol (udp or tcp)"
    batch_size: PositiveInt = 625
    "Number of packets in batch"
    max_payload_size: PositiveInt = 4160
    "Maximum payload size expected from sender"


@dataclasses.dataclass
class AdvancedNetworkOperatorParams:
    """Advanced network operator parameters"""

    cfg: typing.Optional[dict] = None


def build_config_parser():
    parser = jsonargparse.ArgumentParser(
        prog="vsword_recorder",
        description="Process and record RF data for the VSWORD receiver",
        default_env=True,
    )
    # special config argument to load from yaml file
    parser.add_argument("--config", action="config")
    # operator arguments
    parser.add_class_arguments(SchedulerParams, "scheduler")
    parser.add_class_arguments(PipelineParams, "pipeline")
    parser.add_class_arguments(BasicNetworkOperatorParams, "basic_network")
    parser.add_class_arguments(AdvancedNetworkOperatorParams, "advanced_network")
    parser.add_class_arguments(
        NetConnectorAdvancedParams,
        "packet_common",
    )
    parser.add_class_arguments(
        NetConnectorAdvancedParams,
        "packet0",
        default=NetConnectorAdvancedParams(
            max_packet_size=4160,
            num_subchannels=8,
            freq_idx_scaling=2000000,
            freq_idx_offset=3000000,
            queue_id=0,
        ),
    )
    parser.add_class_arguments(
        NetConnectorAdvancedParams,
        "packet1",
        default=NetConnectorAdvancedParams(
            max_packet_size=4160,
            num_subchannels=8,
            freq_idx_scaling=2000000,
            freq_idx_offset=3000000,
            queue_id=1,
        ),
    )
    parser.add_class_arguments(
        SubchannelSelectParams,
        "selector0",
        default=SubchannelSelectParams(subchannel_idx=[0, 1, 2, 3, 4, 5]),
    )
    parser.add_class_arguments(
        SubchannelSelectParams,
        "selector1",
        default=SubchannelSelectParams(subchannel_idx=[0, 1, 2, 3, 4, 5]),
    )
    parser.add_class_arguments(RotatorScheduledParams, "rotator0")
    parser.add_class_arguments(
        RotatorScheduledParams,
        "rotator1",
        default=RotatorScheduledParams(
            cycle_duration_secs=1,
            cycle_start_timestamp=0,
            schedule=[{"start": 0, "freq": 31.65e6}],
        ),
    )
    parser.add_class_arguments(
        ResamplePolyParams,
        "resampler0",
        default=ResamplePolyParams(
            up=1,
            down=20,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=98.35,
        ),
    )
    parser.add_class_arguments(
        ResamplePolyParams,
        "resampler1",
        default=ResamplePolyParams(
            up=1,
            down=2,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=98.35,
        ),
    )
    parser.add_class_arguments(
        DigitalRFSinkParams,
        "drf_sink0",
        default=DigitalRFSinkParams(channel_dir="emvsis/vs"),
    )
    parser.add_class_arguments(
        DigitalRFSinkParams,
        "drf_sink1",
        default=DigitalRFSinkParams(channel_dir="zephyr/vs"),
    )
    parser.add_argument(
        "--metadata0", type=typing.Optional[dict[str, typing.Any]], default=None
    )
    parser.add_argument(
        "--metadata1", type=typing.Optional[dict[str, typing.Any]], default=None
    )
    parser.add_class_arguments(
        SpectrogramParams,
        "spectrogram0",
        default=SpectrogramParams(nperseg=1000),
    )
    parser.add_class_arguments(
        SpectrogramParams,
        "spectrogram1",
        default=SpectrogramParams(nperseg=1000),
    )
    parser.add_class_arguments(
        SpectrogramMQTTParams,
        "spectrogram_mqtt0",
        default=SpectrogramMQTTParams(
            service_name="spectrogram-emvsis",
            status_topic="dt/vsword/{service_name}/{node_id}/status",
            data_topic="dt/vsword/{service_name}/{node_id}/data",
            payload_format="f32buffer",
        ),
    )
    parser.add_class_arguments(
        SpectrogramMQTTParams,
        "spectrogram_mqtt1",
        default=SpectrogramMQTTParams(
            service_name="spectrogram-zephyr",
            status_topic="dt/vsword/{service_name}/{node_id}/status",
            data_topic="dt/vsword/{service_name}/{node_id}/data",
            payload_format="f32buffer",
        ),
    )
    parser.add_class_arguments(
        SpectrogramOutputParams,
        "spectrogram_output0",
        default=SpectrogramOutputParams(plot_subdir="spectrograms/emvsis"),
    )
    parser.add_class_arguments(
        SpectrogramOutputParams,
        "spectrogram_output1",
        default=SpectrogramOutputParams(plot_subdir="spectrograms/zephyr"),
    )

    # non-operator arguments that we use from recorder_service
    parser.add_argument(
        "--ram_ringbuffer_path", type=typing.Optional[os.PathLike], default="."
    )
    parser.link_arguments(
        "ram_ringbuffer_path", "drf_sink0.output_path", apply_on="parse"
    )
    parser.link_arguments(
        "ram_ringbuffer_path", "drf_sink1.output_path", apply_on="parse"
    )
    parser.add_argument("--output_path", type=typing.Optional[os.PathLike], default=".")
    parser.link_arguments(
        "output_path", "spectrogram_output0.output_path", apply_on="parse"
    )
    parser.link_arguments(
        "output_path", "spectrogram_output1.output_path", apply_on="parse"
    )

    return parser


class App(holoscan.core.Application):
    def add_channel_flow(self, ch_idx):
        cuda_stream_pool = holoscan.resources.CudaStreamPool(
            self,
            name=f"ch{ch_idx}_stream_pool",
            stream_flags=1,  # cudaStreamNonBlocking
            stream_priority=0,
            reserved_size=1,
            max_size=0,
        )
        priority_stream_pool = holoscan.resources.CudaStreamPool(
            self,
            name=f"ch{ch_idx}_priority_stream_pool",
            stream_flags=1,  # cudaStreamNonBlocking
            stream_priority=-2,  # lower means higher priority
            reserved_size=1,
            max_size=0,
        )
        thread_pool = self.make_thread_pool(f"ch{ch_idx}_pinned_thread_pool", 1)
        # Holoscan 3.2 doesn't support add_realtime() for thread pool, so skip for now
        # network_thread_pool = self.make_thread_pool(f"ch{ch_idx}_network_thread_pool", 2)

        packet_kwargs = self.kwargs(f"packet{ch_idx}")
        net_connector_rx = rf_array.NetConnectorAdvanced(
            self,
            holoscan.conditions.PeriodicCondition(
                self,
                recess_period=10000000,
                policy=holoscan.conditions.PeriodicConditionPolicy.NO_CATCH_UP_MISSED_TICKS,
            ),
            priority_stream_pool,
            name=f"net_connector_rx{ch_idx}",
            advanced_network=str(self.from_config("advanced_network")),
            **packet_kwargs,
        )
        net_connector_rx.spec.outputs["rf_out"].condition(
            holoscan.core.ConditionType.DOWNSTREAM_MESSAGE_AFFORDABLE,
            min_size=packet_kwargs.get("buffer_size", 4),
        ).connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=2 * packet_kwargs.get("buffer_size", 4),
            policy=0,  # pop
        )
        # Holoscan 3.2 doesn't support add_realtime() for thread pool, so skip for now
        # network_thread_pool.add_realtime(
        #     net_connector_rx,
        #     sched_policy=holoscan.resources.SchedulingPolicy.SCHED_RR,
        #     pin_operator=True,
        #     sched_priority=10,
        # )
        thread_pool.add(net_connector_rx, True)

        last_chunk_shape = (
            self.kwargs(f"packet{ch_idx}")["num_samples"],
            self.kwargs(f"packet{ch_idx}")["num_subchannels"],
        )
        last_buffer_capacity = 2 * packet_kwargs.get("buffer_size", 4)
        last_op = net_connector_rx

        if self.kwargs("pipeline")[f"selector{ch_idx}"]:
            selector = rf_array.SubchannelSelect_sc16(
                self,
                cuda_stream_pool,
                name=f"selector{ch_idx}",
                **self.kwargs(f"selector{ch_idx}"),
            )
            selector.spec.inputs["rf_in"].connector(
                holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                capacity=last_buffer_capacity,
                policy=0,  # pop
            )
            thread_pool.add(selector, True)
            self.add_flow(last_op, selector, {("rf_out", "rf_in")})
            last_op = selector
            last_buffer_capacity = 1
            last_chunk_shape = (
                last_chunk_shape[0],
                len(self.kwargs(f"selector{ch_idx}")["subchannel_idx"]),
            )

        if self.kwargs("pipeline")[f"converter{ch_idx}"]:
            converter = rf_array.TypeConversionComplexIntToFloat(
                self,
                cuda_stream_pool,
                name=f"converter{ch_idx}",
            )
            converter.spec.inputs["rf_in"].connector(
                holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                capacity=last_buffer_capacity,
                policy=0,  # pop
            )
            thread_pool.add(converter, True)
            self.add_flow(last_op, converter, {("rf_out", "rf_in")})
            last_op = converter
            last_buffer_capacity = 1

            if self.kwargs("pipeline")[f"rotator{ch_idx}"]:
                rotator = rf_array.RotatorScheduled(
                    self,
                    cuda_stream_pool,
                    name=f"rotator{ch_idx}",
                    **self.kwargs(f"rotator{ch_idx}"),
                )
                thread_pool.add(rotator, True)
                self.add_flow(last_op, rotator)
                last_op = rotator

            if self.kwargs("pipeline")[f"resampler{ch_idx}"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **self.kwargs(f"resampler{ch_idx}")
                )
                resampler = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name=f"resampler{ch_idx}", **resample_kwargs
                )
                thread_pool.add(resampler, True)
                self.add_flow(last_op, resampler)
                last_op = resampler
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if self.kwargs("pipeline")[f"spectrogram{ch_idx}"]:
                spectrogram = Spectrogram(
                    self,
                    cuda_stream_pool,
                    name=f"spectrogram{ch_idx}",
                    **add_chunk_kwargs(
                        last_chunk_shape, **self.kwargs(f"spectrogram{ch_idx}")
                    ),
                )
                # Queue policy is currently set by specifying a connector in setup()
                # # drop old messages rather than get backed up by slow
                # # downstream operators
                # spectrogram.queue_policy(
                #     port_name="spec_out",
                #     port_type=holoscan.core.IOSpec.IOType.OUTPUT,
                #     policy=holoscan.core.IOSpec.QueuePolicy.POP,
                # )
                thread_pool.add(spectrogram, True)
                self.add_flow(last_op, spectrogram)

                if self.kwargs("pipeline")[f"spectrogram_mqtt{ch_idx}"]:
                    spec_mqtt_kwargs = self.kwargs(f"spectrogram_mqtt{ch_idx}")
                    spec_mqtt_kwargs.update(
                        spec_sample_cadence=spectrogram.spec_sample_cadence,
                    )
                    spectrogram_mqtt = SpectrogramMQTT(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name=f"spectrogram_mqtt{ch_idx}_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name=f"spectrogram_mqtt{ch_idx}_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name=f"spectrogram_mqtt{ch_idx}",
                        **spec_mqtt_kwargs,
                    )
                    thread_pool.add(spectrogram_mqtt, True)
                    self.add_flow(spectrogram, spectrogram_mqtt)

                if self.kwargs("pipeline")[f"spectrogram_output{ch_idx}"]:
                    spec_out_kwargs = self.kwargs(f"spectrogram_output{ch_idx}")
                    spec_out_kwargs.update(
                        nfft=spectrogram.nfft,
                        spec_sample_cadence=spectrogram.spec_sample_cadence,
                        num_subchannels=spectrogram.num_subchannels,
                        data_subdir=(
                            f"{self.kwargs(f'drf_sink{ch_idx}')['channel_dir']}_spectrogram"
                        ),
                    )
                    spectrogram_output = SpectrogramOutput(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name=f"spectrogram_output{ch_idx}_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name=f"spectrogram_output{ch_idx}_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name=f"spectrogram_output{ch_idx}",
                        **spec_out_kwargs,
                    )
                    thread_pool.add(spectrogram_output, True)
                    self.add_flow(spectrogram, spectrogram_output)

        if self.kwargs("pipeline")[f"digital_rf{ch_idx}"]:
            if self.kwargs("pipeline")[f"converter{ch_idx}"]:
                drf_sink = rf_array.DigitalRFSink_fc32(
                    self,
                    cuda_stream_pool,
                    name=f"drf_sink{ch_idx}",
                    **add_chunk_kwargs(
                        last_chunk_shape, **self.kwargs(f"drf_sink{ch_idx}")
                    ),
                )
                drf_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(drf_sink, True)
                self.add_flow(last_op, drf_sink, {("rf_out", "rf_in")})
            else:
                drf_sink = rf_array.DigitalRFSink_sc16(
                    self,
                    cuda_stream_pool,
                    name=f"drf_sink{ch_idx}",
                    **add_chunk_kwargs(
                        last_chunk_shape, **self.kwargs(f"drf_sink{ch_idx}")
                    ),
                )
                drf_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(drf_sink, True)
                self.add_flow(last_op, drf_sink, {("rf_out", "rf_in")})

            if self.kwargs("pipeline")[f"metadata{ch_idx}"]:
                dmd_sink = DigitalMetadataSink(
                    self,
                    name=f"dmd_sink{ch_idx}",
                    output_path=self.kwargs(f"drf_sink{ch_idx}")["output_path"],
                    metadata_dir=f"{self.kwargs(f'drf_sink{ch_idx}')['channel_dir']}/metadata",
                    subdir_cadence_secs=self.kwargs(f"drf_sink{ch_idx}")[
                        "subdir_cadence_secs"
                    ],
                    file_cadence_secs=self.kwargs(f"drf_sink{ch_idx}")[
                        "file_cadence_millisecs"
                    ]
                    // 1000,
                    uuid=self.kwargs(f"drf_sink{ch_idx}")["uuid"],
                    filename_prefix="metadata",
                    metadata=self.kwargs(f"metadata{ch_idx}"),
                )
                dmd_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(dmd_sink, True)
                self.add_flow(last_op, dmd_sink)

    def compose(self):
        for ch_idx in range(2):
            self.add_channel_flow(ch_idx)


def main():
    parser = build_config_parser()
    cfg = parser.parse_args()

    logger = logging.getLogger("holoscan.vsword_recorder")

    # We have a parsed configuration (using jsonargparse), but the holoscan app wants
    # to read all of its configuration parameters from a YAML file, so we write out
    # the configuration to a file in the temporary directory and feed it that
    tmp_config_dir = tempfile.TemporaryDirectory(prefix=os.path.basename(__file__))
    config_path = pathlib.Path(tmp_config_dir.name) / "recorder_config.yaml"
    logger.debug(f"Writing temporary config file to {config_path}")
    with config_path.open("w") as f:
        f.write(
            parser.dump(
                cfg,
                format="yaml",
                skip_none=True,
                skip_default=False,
                skip_link_targets=False,
            )
        )

    app = App([sys.executable, sys.argv[0]])
    app.logger = logger
    app.config(str(config_path))

    scheduler = holoscan.schedulers.EventBasedScheduler(
        app,
        name="event-based-scheduler",
        **app.kwargs("scheduler"),
    )
    app.scheduler(scheduler)

    def sigterm_handler(signal, frame):
        logger.info("Received SIGTERM, cleaning up")
        sys.stdout.flush()
        tmp_config_dir.cleanup()
        sys.exit(128 + signal)

    signal.signal(signal.SIGTERM, sigterm_handler)

    try:
        app.run()
    except KeyboardInterrupt:
        # catch keyboard interrupt and simply exit
        tmp_config_dir.cleanup()
        logger.info("Done")
        sys.stdout.flush()
        # Holoscan graph execution framework handles all cleanup
        # so we just need to exit immediately without further Python cleanup
        # (which would result in a segfault from double free)
        os._exit(0)
    except SystemExit as e:
        tmp_config_dir.cleanup()
        # Holoscan graph execution framework handles all cleanup
        # so we just need to exit immediately without further Python cleanup
        # (which would result in a segfault from double free)
        os._exit(e.code)
    else:
        tmp_config_dir.cleanup()


if __name__ == "__main__":
    main()
