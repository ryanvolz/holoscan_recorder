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
from holohub import advanced_network_common, rf_array
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

    worker_thread_number: PositiveInt = 16
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
    parser.add_argument("--scheduler", type=SchedulerParams)
    parser.add_argument("--pipeline", type=PipelineParams)
    parser.add_argument("--basic_network", type=BasicNetworkOperatorParams)
    parser.add_argument("--advanced_network", type=AdvancedNetworkOperatorParams)
    parser.add_argument(
        "--packet_common",
        type=NetConnectorAdvancedParams,
    )
    parser.add_argument(
        "--packet0",
        type=NetConnectorAdvancedParams,
        default=NetConnectorAdvancedParams(
            max_packet_size=4160,
            num_subchannels=8,
            freq_idx_scaling=2000000,
            freq_idx_offset=3000000,
            queue_id=0,
        ),
    )
    parser.add_argument(
        "--packet1",
        type=NetConnectorAdvancedParams,
        default=NetConnectorAdvancedParams(
            max_packet_size=4160,
            num_subchannels=8,
            freq_idx_scaling=2000000,
            freq_idx_offset=3000000,
            queue_id=1,
        ),
    )
    parser.add_argument(
        "--selector0",
        type=SubchannelSelectParams,
        default=SubchannelSelectParams(subchannel_idx=[0, 1, 2, 3, 4, 5]),
    )
    parser.add_argument(
        "--selector1",
        type=SubchannelSelectParams,
        default=SubchannelSelectParams(subchannel_idx=[0, 1, 2, 3, 4, 5]),
    )
    parser.add_argument("--rotator0", type=RotatorScheduledParams)
    parser.add_argument(
        "--rotator1",
        type=RotatorScheduledParams,
        default=RotatorScheduledParams(
            cycle_duration_secs=1,
            cycle_start_timestamp=0,
            schedule=[{"start": 0, "freq": 31.65e6}],
        ),
    )
    parser.add_argument(
        "--resampler0",
        type=ResamplePolyParams,
        default=ResamplePolyParams(
            up=1,
            down=20,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=98.35,
        ),
    )
    parser.add_argument(
        "--resampler1",
        type=ResamplePolyParams,
        default=ResamplePolyParams(
            up=1,
            down=2,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=98.35,
        ),
    )
    parser.add_argument(
        "--drf_sink0",
        type=DigitalRFSinkParams,
        default=DigitalRFSinkParams(channel_dir="emvsis/vs"),
    )
    parser.add_argument(
        "--drf_sink1",
        type=DigitalRFSinkParams,
        default=DigitalRFSinkParams(channel_dir="zephyr/vs"),
    )
    parser.add_argument(
        "--metadata0", type=typing.Optional[dict[str, typing.Any]], default=None
    )
    parser.add_argument(
        "--metadata1", type=typing.Optional[dict[str, typing.Any]], default=None
    )
    parser.add_argument(
        "--spectrogram0",
        type=SpectrogramParams,
        default=SpectrogramParams(nperseg=1000),
    )
    parser.add_argument(
        "--spectrogram1",
        type=SpectrogramParams,
        default=SpectrogramParams(nperseg=1000),
    )
    parser.add_argument(
        "--spectrogram_mqtt0",
        type=SpectrogramMQTTParams,
        default=SpectrogramMQTTParams(
            service_name="spectrogram_emvsis",
            status_topic="{service_name}/status",
            data_topic="{service_name}/data",
        ),
    )
    parser.add_argument(
        "--spectrogram_mqtt1",
        type=SpectrogramMQTTParams,
        default=SpectrogramMQTTParams(
            service_name="spectrogram_zephyr",
            status_topic="{service_name}/status",
            data_topic="{service_name}/data",
        ),
    )
    parser.add_argument(
        "--spectrogram_output0",
        type=SpectrogramOutputParams,
        default=SpectrogramOutputParams(plot_subdir="spectrograms/emvsis"),
    )
    parser.add_argument(
        "--spectrogram_output1",
        type=SpectrogramOutputParams,
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
    def compose(self):
        cuda_stream_pool = holoscan.resources.CudaStreamPool(
            self,
            name="stream_pool",
            stream_flags=1,  # cudaStreamNonBlocking
            stream_priority=0,
            reserved_size=1,
            max_size=0,
        )

        advanced_network_common.adv_net_init(self.from_config("advanced_network"))

        # sample flow 0

        packet0_kwargs = self.kwargs("packet0")
        net_connector_rx0 = rf_array.NetConnectorAdvanced(
            self,
            cuda_stream_pool,
            name="net_connector_rx0",
            **packet0_kwargs,
        )
        net_connector_rx0.spec.outputs["rf_out"].connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=packet0_kwargs.get("buffer_size", 4),
            policy=0,  # pop
        )

        last_chunk_shape = (
            self.kwargs("packet0")["num_samples"],
            self.kwargs("packet0")["num_subchannels"],
        )
        last_op = net_connector_rx0

        if self.kwargs("pipeline")["selector0"]:
            selector0 = rf_array.SubchannelSelect_sc16(
                self, cuda_stream_pool, name="selector0", **self.kwargs("selector0")
            )
            self.add_flow(last_op, selector0)
            last_op = selector0
            last_chunk_shape = (
                last_chunk_shape[0],
                len(self.kwargs("selector0")["subchannel_idx"]),
            )

        if self.kwargs("pipeline")["converter0"]:
            converter0 = rf_array.TypeConversionComplexIntToFloat(
                self,
                cuda_stream_pool,
                name="converter0",
            )
            self.add_flow(last_op, converter0)
            last_op = converter0

            if self.kwargs("pipeline")["rotator0"]:
                rotator0 = rf_array.RotatorScheduled(
                    self, cuda_stream_pool, name="rotator0", **self.kwargs("rotator0")
                )
                self.add_flow(last_op, rotator0)
                last_op = rotator0

            if self.kwargs("pipeline")["resampler0"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **self.kwargs("resampler0")
                )
                resampler0 = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name="resampler0", **resample_kwargs
                )
                self.add_flow(last_op, resampler0)
                last_op = resampler0
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if self.kwargs("pipeline")["spectrogram0"]:
                spectrogram0 = Spectrogram(
                    self,
                    cuda_stream_pool,
                    name="spectrogram0",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("spectrogram0")),
                )
                # Queue policy is currently set by specifying a connector in setup()
                # # drop old messages rather than get backed up by slow
                # # downstream operators
                # spectrogram.queue_policy(
                #     port_name="spec_out",
                #     port_type=holoscan.core.IOSpec.IOType.OUTPUT,
                #     policy=holoscan.core.IOSpec.QueuePolicy.POP,
                # )
                self.add_flow(last_op, spectrogram0)

                if self.kwargs("pipeline")["spectrogram_mqtt0"]:
                    spec_mqtt_kwargs = self.kwargs("spectrogram_mqtt0")
                    spec_mqtt_kwargs.update(
                        spec_sample_cadence=spectrogram0.spec_sample_cadence,
                    )
                    spectrogram_mqtt0 = SpectrogramMQTT(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name="spectrogram_mqtt0_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name="spectrogram_mqtt0_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name="spectrogram_mqtt0",
                        **spec_mqtt_kwargs,
                    )
                    self.add_flow(spectrogram0, spectrogram_mqtt0)

                if self.kwargs("pipeline")["spectrogram_output0"]:
                    spec_out_kwargs = self.kwargs("spectrogram_output0")
                    spec_out_kwargs.update(
                        nfft=spectrogram0.nfft,
                        spec_sample_cadence=spectrogram0.spec_sample_cadence,
                        num_subchannels=spectrogram0.num_subchannels,
                        data_subdir=(
                            f"{self.kwargs('drf_sink0')['channel_dir']}_spectrogram"
                        ),
                    )
                    spectrogram_output0 = SpectrogramOutput(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name="spectrogram_output_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name="spectrogram_output_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name="spectrogram_output0",
                        **spec_out_kwargs,
                    )
                    self.add_flow(spectrogram0, spectrogram_output0)

        if self.kwargs("pipeline")["digital_rf0"]:
            if self.kwargs("pipeline")["converter0"]:
                drf_sink0 = rf_array.DigitalRFSink_fc32(
                    self,
                    cuda_stream_pool,
                    name="drf_sink0",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("drf_sink0")),
                )
                drf_sink0.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, drf_sink0)
            else:
                drf_sink0 = rf_array.DigitalRFSink_sc16(
                    self,
                    cuda_stream_pool,
                    name="drf_sink0",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("drf_sink0")),
                )
                drf_sink0.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, drf_sink0)

            if self.kwargs("pipeline")["metadata0"]:
                dmd_sink0 = DigitalMetadataSink(
                    self,
                    name="dmd_sink0",
                    output_path=self.kwargs("drf_sink0")["output_path"],
                    metadata_dir=f"{self.kwargs('drf_sink0')['channel_dir']}/metadata",
                    subdir_cadence_secs=self.kwargs("drf_sink0")["subdir_cadence_secs"],
                    file_cadence_secs=self.kwargs("drf_sink0")["file_cadence_millisecs"]
                    // 1000,
                    uuid=self.kwargs("drf_sink0")["uuid"],
                    filename_prefix="metadata",
                    metadata=self.kwargs("metadata0"),
                )
                dmd_sink0.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, dmd_sink0)

        # sample flow 1

        packet1_kwargs = self.kwargs("packet1")
        net_connector_rx1 = rf_array.NetConnectorAdvanced(
            self,
            cuda_stream_pool,
            name="net_connector_rx1",
            **packet1_kwargs,
        )
        net_connector_rx1.spec.outputs["rf_out"].connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=packet1_kwargs.get("buffer_size", 4),
            policy=0,  # pop
        )

        last_chunk_shape = (
            self.kwargs("packet1")["num_samples"],
            self.kwargs("packet1")["num_subchannels"],
        )
        last_op = net_connector_rx1

        if self.kwargs("pipeline")["selector1"]:
            selector1 = rf_array.SubchannelSelect_sc16(
                self, cuda_stream_pool, name="selector1", **self.kwargs("selector1")
            )
            self.add_flow(last_op, selector1)
            last_op = selector1
            last_chunk_shape = (
                last_chunk_shape[0],
                len(self.kwargs("selector1")["subchannel_idx"]),
            )

        if self.kwargs("pipeline")["converter1"]:
            converter1 = rf_array.TypeConversionComplexIntToFloat(
                self,
                cuda_stream_pool,
                name="converter1",
            )
            self.add_flow(last_op, converter1)
            last_op = converter1

            if self.kwargs("pipeline")["rotator1"]:
                rotator1 = rf_array.RotatorScheduled(
                    self, cuda_stream_pool, name="rotator1", **self.kwargs("rotator1")
                )
                self.add_flow(last_op, rotator1)
                last_op = rotator1

            if self.kwargs("pipeline")["resampler1"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **self.kwargs("resampler1")
                )
                resampler1 = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name="resampler1", **resample_kwargs
                )
                self.add_flow(last_op, resampler1)
                last_op = resampler1
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if self.kwargs("pipeline")["spectrogram1"]:
                spectrogram1 = Spectrogram(
                    self,
                    cuda_stream_pool,
                    name="spectrogram1",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("spectrogram1")),
                )
                # Queue policy is currently set by specifying a connector in setup()
                # # drop old messages rather than get backed up by slow
                # # downstream operators
                # spectrogram.queue_policy(
                #     port_name="spec_out",
                #     port_type=holoscan.core.IOSpec.IOType.OUTPUT,
                #     policy=holoscan.core.IOSpec.QueuePolicy.POP,
                # )
                self.add_flow(last_op, spectrogram1)

                if self.kwargs("pipeline")["spectrogram_mqtt1"]:
                    spec_mqtt_kwargs = self.kwargs("spectrogram_mqtt1")
                    spec_mqtt_kwargs.update(
                        spec_sample_cadence=spectrogram1.spec_sample_cadence,
                    )
                    spectrogram_mqtt1 = SpectrogramMQTT(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name="spectrogram_mqtt0_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name="spectrogram_mqtt0_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name="spectrogram_mqtt1",
                        **spec_mqtt_kwargs,
                    )
                    self.add_flow(spectrogram1, spectrogram_mqtt1)

                if self.kwargs("pipeline")["spectrogram_output1"]:
                    spec_out_kwargs = self.kwargs("spectrogram_output1")
                    spec_out_kwargs.update(
                        nfft=spectrogram1.nfft,
                        spec_sample_cadence=spectrogram1.spec_sample_cadence,
                        num_subchannels=spectrogram1.num_subchannels,
                        data_subdir=(
                            f"{self.kwargs('drf_sink1')['channel_dir']}_spectrogram"
                        ),
                    )
                    spectrogram_output1 = SpectrogramOutput(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name="spectrogram_output_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name="spectrogram_output_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name="spectrogram_output1",
                        **spec_out_kwargs,
                    )
                    self.add_flow(spectrogram1, spectrogram_output1)

        if self.kwargs("pipeline")["digital_rf1"]:
            if self.kwargs("pipeline")["converter1"]:
                drf_sink1 = rf_array.DigitalRFSink_fc32(
                    self,
                    cuda_stream_pool,
                    name="drf_sink1",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("drf_sink1")),
                )
                drf_sink1.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, drf_sink1)
            else:
                drf_sink1 = rf_array.DigitalRFSink_sc16(
                    self,
                    cuda_stream_pool,
                    name="drf_sink1",
                    **add_chunk_kwargs(last_chunk_shape, **self.kwargs("drf_sink1")),
                )
                drf_sink1.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, drf_sink1)

            if self.kwargs("pipeline")["metadata1"]:
                dmd_sink1 = DigitalMetadataSink(
                    self,
                    name="dmd_sink1",
                    output_path=self.kwargs("drf_sink1")["output_path"],
                    metadata_dir=f"{self.kwargs('drf_sink1')['channel_dir']}/metadata",
                    subdir_cadence_secs=self.kwargs("drf_sink1")["subdir_cadence_secs"],
                    file_cadence_secs=self.kwargs("drf_sink1")["file_cadence_millisecs"]
                    // 1000,
                    uuid=self.kwargs("drf_sink1")["uuid"],
                    filename_prefix="metadata",
                    metadata=self.kwargs("metadata1"),
                )
                dmd_sink1.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                self.add_flow(last_op, dmd_sink1)

    def cleanup(self):
        # This is not a Holoscan method!
        # We use it to contain code for shutting down the advanced network operator
        try:
            advanced_network_common.shutdown()
        except Exception:
            self.logger.exception("Exception during shutdown!")


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
        app.cleanup()
        tmp_config_dir.cleanup()
        logger.info("Done")
        sys.stdout.flush()
        # Holoscan graph execution framework handles all cleanup
        # so we just need to exit immediately without further Python cleanup
        # (which would result in a segfault from double free)
        os._exit(0)
    except SystemExit as e:
        # nothing cleans up the advanced network operator, so we do it here
        app.cleanup()
        tmp_config_dir.cleanup()
        # Holoscan graph execution framework handles all cleanup
        # so we just need to exit immediately without further Python cleanup
        # (which would result in a segfault from double free)
        os._exit(e.code)
    else:
        app.cleanup()
        tmp_config_dir.cleanup()


if __name__ == "__main__":
    main()
