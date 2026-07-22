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
import msgspec
from holohub import basic_network, rf_array
from holohub.rf_array.digital_metadata import DigitalMetadataSink
from holohub.rf_array.params import (
    DigitalRFSinkParams,
    NetConnectorBasicParams,
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


DEFAULT_BATCH_SIZE = 3125
DEFAULT_MAX_PACKET_SIZE = 8256
DEFAULT_NUM_SUBCHANNELS = 1


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

    selector: bool = False
    "Enable / disable subchannel selector"
    converter: bool = True
    "Enable / disable complex int to float converter"
    rotator: bool = False
    "Enable / disable frequency rotator"
    resampler0: bool = True
    "Enable / disable the first stage resampler"
    resampler1: bool = False
    "Enable / disable the second stage resampler"
    resampler2: bool = True
    "Enable / disable the third stage resampler"
    int_converter: bool = True
    "Enable / disable complex float to int converter"
    digital_rf: bool = True
    "Enable / disable writing output to Digital RF"
    metadata: bool = True
    "Enable / disable writing inherent and user-supplied Digital RF metadata"
    spectrogram: bool = True
    "Enable / disable spectrogram processing"
    spectrogram_mqtt: bool = True
    "Enable / disable spectrogram output over MQTT"
    spectrogram_output: bool = True
    "Enable / disable spectrogram output to files"


@dataclasses.dataclass
class BasicNetworkOperatorParams:
    """Basic network operator parameters"""

    ip_addr: str = "0.0.0.0"
    """IP address to bind to"""
    dst_port: NonNegativeInt = 60133
    "UDP or TCP port to listen on"
    l4_proto: str = "udp"
    "Layer 4 protocol (udp or tcp)"
    batch_size: PositiveInt = DEFAULT_BATCH_SIZE
    "Number of packets in batch"
    max_payload_size: PositiveInt = DEFAULT_MAX_PACKET_SIZE
    "Maximum payload size expected from sender"


class RecorderParser(jsonargparse.ArgumentParser):
    """Parser with aliased arguments for ch0"""

    def add_argument(self, *args, **kwargs):
        if args[0].startswith("--ch0."):
            ch0_alias = f"--{args[0].removeprefix('--ch0.')}"
            if ch0_alias not in args:
                args += (ch0_alias,)
        return super().add_argument(*args, **kwargs)


class RecorderActionConfigFile(jsonargparse.ActionConfigFile):
    """Config file action handling aliased arguments for ch0"""

    def __call__(self, parser, cfg, values, option_string=None):
        try:
            cfg_path = pathlib.Path(values)
        except Exception:
            pass
        else:
            cfg_text = cfg_path.read_text()
            cfg_dict = msgspec.yaml.decode(cfg_text)
            # rewrite config to put top-level channel arguments under "ch0"
            out_dict = {}
            out_dict["ch0"] = cfg_dict.pop("ch0", {})
            for k, v in cfg_dict.items():
                if k in (
                    "basic_network",
                    "drf_sink",
                    "enabled",
                    "metadata",
                    "packet",
                    "pipeline",
                    "resampler0",
                    "resampler1",
                    "resampler2",
                    "rotator",
                    "selector",
                    "spectrogram",
                    "spectrogram_mqtt",
                    "spectrogram_output",
                ):
                    out_dict["ch0"][k] = v
                else:
                    out_dict[k] = v
            values = msgspec.yaml.encode(out_dict).decode()
        return super().__call__(parser, cfg, values, option_string=option_string)


def build_config_parser():
    parser = RecorderParser(
        prog="mep_recorder",
        description="Process and record RF data for the SpectrumX Mobile Experiment Platform (MEP)",
        default_env=True,
    )
    # special config argument to load from yaml file
    parser.add_argument("--config", action=RecorderActionConfigFile)
    # operator arguments
    parser.add_class_arguments(SchedulerParams, "scheduler")

    # non-operator arguments that we use from recorder_service
    parser.add_argument(
        "--ram_ringbuffer_path", type=typing.Optional[os.PathLike], default="."
    )
    parser.add_argument("--output_path", type=typing.Optional[os.PathLike], default=".")

    # add channel-based arguments
    for ch_idx in range(4):
        build_channel_subparser(parser, f"ch{ch_idx}")

    return parser


def build_channel_subparser(parser, ch):
    parser.add_argument(
        f"--{ch}.enabled", type=bool, default=True if ch == "ch0" else False
    )
    parser.add_class_arguments(PipelineParams, f"{ch}.pipeline")
    parser.add_class_arguments(
        BasicNetworkOperatorParams,
        f"{ch}.basic_network",
        default=BasicNetworkOperatorParams(
            dst_port={
                "ch0": 60134,
                "ch1": 60133,
                "ch2": 60132,
                "ch3": 60131,
            }.get(ch, 60134),
        ),
    )
    parser.add_class_arguments(
        NetConnectorBasicParams,
        f"{ch}.packet",
        default=NetConnectorBasicParams(
            batch_size=DEFAULT_BATCH_SIZE,
            batch_capacity=10,
            max_packet_size=DEFAULT_MAX_PACKET_SIZE,
            num_subchannels=DEFAULT_NUM_SUBCHANNELS,
            num_samples=6400000,
            buffer_size=5,
            freq_idx_scaling=1000,
            freq_idx_offset=0,
            apply_conjugate=False,
            spoof_header=False,
            packet_skip_bytes=64,
            header_metadata={
                "start_sample_idx": 0,
                "sample_rate_numerator": 64000000,
                "sample_rate_denominator": 1,
                "freq_idx": 0,
                "num_subchannels": DEFAULT_NUM_SUBCHANNELS,
                "pkt_samples": 2048,
                "bits_per_int": 16,
                "is_complex": 1,
            },
        ),
    )
    parser.add_class_arguments(
        SubchannelSelectParams,
        f"{ch}.selector",
        default=SubchannelSelectParams(subchannel_idx=[0]),
    )
    parser.add_class_arguments(
        RotatorScheduledParams,
        f"{ch}.rotator",
        default=RotatorScheduledParams(
            cycle_duration_secs=1,
            cycle_start_timestamp=0,
            # dummy schedule, negative frequencies are ignored
            schedule=[{"start": 0, "freq": -1.0}],
        ),
    )
    parser.add_class_arguments(
        ResamplePolyParams,
        f"{ch}.resampler0",
        default=ResamplePolyParams(
            up=1,
            down=8,
            outrate_cutoff=1.0,
            # transition_width: 2 * (cutoff - 1 / remaining_dec)
            #                   2 * (1.0 - 1 / 8) = 1.75
            outrate_transition_width=1.75,
            # tweak attenuation to get desired number of taps indicated by
            # scipy.signal.kaiserord(attenuation_db, outrate_transition_width / down)
            attenuation_db=105,
        ),
    )
    parser.add_class_arguments(
        ResamplePolyParams,
        f"{ch}.resampler1",
        default=ResamplePolyParams(
            up=5,
            down=16,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=99.65,
        ),
    )
    parser.add_class_arguments(
        ResamplePolyParams,
        f"{ch}.resampler2",
        default=ResamplePolyParams(
            up=1,
            down=8,
            outrate_cutoff=1.0,
            outrate_transition_width=0.2,
            attenuation_db=99.475,
        ),
    )
    parser.add_class_arguments(
        DigitalRFSinkParams,
        f"{ch}.drf_sink",
        default=DigitalRFSinkParams(
            channel_dir=f"{ch}",
            subdir_cadence_secs=3600,
            file_cadence_millisecs=1000,
        ),
    )
    parser.add_argument(
        f"--{ch}.metadata",
        type=typing.Optional[dict[str, typing.Any]],
        default={
            "receiver": {
                "description": "MEP recorder",
            },
        },
    )
    parser.add_class_arguments(
        SpectrogramParams,
        f"{ch}.spectrogram",
        default=SpectrogramParams(
            window="hann",
            nperseg=1600,
            noverlap=None,
            nfft=None,
            detrend=False,
            reduce_op="max",
            num_spectra_per_chunk=1,
        ),
    )
    parser.add_class_arguments(
        SpectrogramMQTTParams,
        f"{ch}.spectrogram_mqtt",
        default=SpectrogramMQTTParams(
            input_buffer_capacity=10,
            service_name=f"recorder_fft_{ch}",
            status_topic="{service_name}/status",
            data_topic="radiohound/clients/data/{node_id}",
            mqtt_host="localhost",
            mqtt_port=1883,
            mqtt_keepalive=60,
            payload_format="radiohound",
        ),
    )
    parser.add_class_arguments(
        SpectrogramOutputParams,
        f"{ch}.spectrogram_output",
        default=SpectrogramOutputParams(
            num_spectra_per_output=600,
            figsize=[6.4, 4.8],
            dpi=200,
            col_wrap=1,
            cmap="viridis",
            snr_db_min=-5,
            snr_db_max=20,
            plot_subdir=f"{ch}_spectrogram_images",
        ),
    )

    # link non-operator arguments that we use from recorder_service
    parser.link_arguments(
        "ram_ringbuffer_path",
        f"{ch}.drf_sink.output_path",
        apply_on="parse",
    )
    parser.link_arguments(
        "output_path",
        f"{ch}.spectrogram_output.output_path",
        apply_on="parse",
    )

    return parser


class App(holoscan.core.Application):
    def compose(self):
        for ch_idx in range(4):
            self.add_channel_flow(f"ch{ch_idx}")

    def add_channel_flow(self, ch):
        ch_kwargs = self.kwargs(ch)
        if not ch_kwargs or not ch_kwargs["enabled"]:
            return

        cuda_stream_pool = holoscan.resources.CudaStreamPool(
            self,
            name=f"{ch}_stream_pool",
            stream_flags=1,  # cudaStreamNonBlocking
            stream_priority=0,
            reserved_size=1,
            max_size=0,
        )
        priority_stream_pool = holoscan.resources.CudaStreamPool(
            self,
            name=f"{ch}_priority_stream_pool",
            stream_flags=1,  # cudaStreamNonBlocking
            stream_priority=-2,  # lower means higher priority
            reserved_size=1,
            max_size=0,
        )
        thread_pool = self.make_thread_pool(f"{ch}_pinned_thread_pool", 1)
        network_thread_pool = self.make_thread_pool(f"{ch}_network_thread_pool", 2)

        basic_net_rx = basic_network.BasicNetworkOpRx(
            self,
            holoscan.conditions.PeriodicCondition(
                self,
                recess_period=10000000,
                policy=holoscan.conditions.PeriodicConditionPolicy.NO_CATCH_UP_MISSED_TICKS,
            ),
            name=f"{ch}_basic_network_rx",
            **ch_kwargs["basic_network"],
        )
        basic_net_rx.spec.outputs["burst_out"].condition(
            holoscan.core.ConditionType.DOWNSTREAM_MESSAGE_AFFORDABLE,
            min_size=1,
        ).connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=ch_kwargs["packet"].get("batch_capacity", 4),
        )
        network_thread_pool.add_realtime(
            basic_net_rx,
            sched_policy=holoscan.resources.SchedulingPolicy.SCHED_DEADLINE,
            pin_operator=True,
            sched_runtime=100000,
            sched_deadline=200000,
            sched_period=200000,
        )

        packet_kwargs = ch_kwargs["packet"]
        net_connector_rx = rf_array.NetConnectorBasic(
            self,
            holoscan.conditions.PeriodicCondition(
                self,
                recess_period=10000000,
                policy=holoscan.conditions.PeriodicConditionPolicy.NO_CATCH_UP_MISSED_TICKS,
            ),
            priority_stream_pool,
            name=f"{ch}_net_connector_rx",
            **packet_kwargs,
        )
        net_connector_rx.spec.inputs["burst_in"].connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=packet_kwargs.get("batch_capacity", 4),
            policy=0,  # pop
        )
        net_connector_rx.spec.outputs["rf_out"].condition(
            holoscan.core.ConditionType.DOWNSTREAM_MESSAGE_AFFORDABLE,
            min_size=packet_kwargs.get("buffer_size", 4),
        ).connector(
            holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
            capacity=2 * packet_kwargs.get("buffer_size", 4),
            policy=0,  # pop
        )
        network_thread_pool.add_realtime(
            net_connector_rx,
            sched_policy=holoscan.resources.SchedulingPolicy.SCHED_RR,
            pin_operator=True,
            sched_priority=10,
        )
        self.add_flow(basic_net_rx, net_connector_rx, {("burst_out", "burst_in")})

        last_chunk_shape = (
            ch_kwargs["packet"]["num_samples"],
            ch_kwargs["packet"]["num_subchannels"],
        )
        last_buffer_capacity = 2 * packet_kwargs.get("buffer_size", 4)
        last_op = net_connector_rx

        if ch_kwargs["pipeline"]["selector"]:
            selector = rf_array.SubchannelSelect_sc16(
                self, cuda_stream_pool, name=f"{ch}_selector", **ch_kwargs["selector"]
            )
            selector.spec.inputs["rf_in"].connector(
                holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                capacity=last_buffer_capacity,
                policy=0,  # pop
            )
            thread_pool.add(selector, True)
            self.add_flow(last_op, selector)
            last_op = selector
            last_buffer_capacity = 1
            last_chunk_shape = (
                last_chunk_shape[0],
                len(ch_kwargs["selector"]["subchannel_idx"]),
            )

        if ch_kwargs["pipeline"]["converter"]:
            converter = rf_array.TypeConversionComplexIntToFloat(
                self,
                cuda_stream_pool,
                name=f"{ch}_converter",
            )
            converter.spec.inputs["rf_in"].connector(
                holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                capacity=last_buffer_capacity,
                policy=0,  # pop
            )
            thread_pool.add(converter, True)
            self.add_flow(last_op, converter)
            last_op = converter
            last_buffer_capacity = 1

            if ch_kwargs["pipeline"]["rotator"]:
                rotator = rf_array.RotatorScheduled(
                    self, cuda_stream_pool, name=f"{ch}_rotator", **ch_kwargs["rotator"]
                )
                thread_pool.add(rotator, True)
                self.add_flow(last_op, rotator)
                last_op = rotator

            if ch_kwargs["pipeline"]["resampler0"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **ch_kwargs["resampler0"]
                )
                resampler0 = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name=f"{ch}_resampler0", **resample_kwargs
                )
                thread_pool.add(resampler0, True)
                self.add_flow(last_op, resampler0)
                last_op = resampler0
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if ch_kwargs["pipeline"]["resampler1"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **ch_kwargs["resampler1"]
                )
                resampler1 = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name=f"{ch}_resampler1", **resample_kwargs
                )
                thread_pool.add(resampler1, True)
                self.add_flow(last_op, resampler1)
                last_op = resampler1
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if ch_kwargs["pipeline"]["resampler2"]:
                resample_kwargs = add_chunk_kwargs(
                    last_chunk_shape, **ch_kwargs["resampler2"]
                )
                resampler2 = rf_array.ResamplePoly(
                    self, cuda_stream_pool, name=f"{ch}_resampler2", **resample_kwargs
                )
                thread_pool.add(resampler2, True)
                self.add_flow(last_op, resampler2)
                last_op = resampler2
                last_chunk_shape = (
                    last_chunk_shape[0]
                    * resample_kwargs["up"]
                    // resample_kwargs["down"],
                    last_chunk_shape[1],
                )

            if ch_kwargs["pipeline"]["spectrogram"]:
                spectrogram = Spectrogram(
                    self,
                    cuda_stream_pool,
                    name=f"{ch}_spectrogram",
                    **add_chunk_kwargs(last_chunk_shape, **ch_kwargs["spectrogram"]),
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

                if ch_kwargs["pipeline"]["spectrogram_mqtt"]:
                    spec_mqtt_kwargs = ch_kwargs["spectrogram_mqtt"]
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
                        #     name="spectrogram_mqtt_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name="spectrogram_mqtt_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name=f"{ch}_spectrogram_mqtt",
                        **spec_mqtt_kwargs,
                    )
                    thread_pool.add(spectrogram_mqtt, True)
                    self.add_flow(spectrogram, spectrogram_mqtt)

                if ch_kwargs["pipeline"]["spectrogram_output"]:
                    spec_out_kwargs = ch_kwargs["spectrogram_output"]
                    spec_out_kwargs.update(
                        nfft=spectrogram.nfft,
                        spec_sample_cadence=spectrogram.spec_sample_cadence,
                        num_subchannels=spectrogram.num_subchannels,
                        data_subdir=(
                            f"{ch_kwargs['drf_sink']['channel_dir']}_spectrogram"
                        ),
                    )
                    spectrogram_output = SpectrogramOutput(
                        self,
                        ## CudaStreamCondition doesn't work with a message queue size
                        ## larger than 1, so get by without it for now
                        # holoscan.conditions.MessageAvailableCondition(
                        #     self,
                        #     receiver="spec_in",
                        #     name=f"{ch}_spectrogram_output_message_available",
                        # ),
                        # holoscan.conditions.CudaStreamCondition(
                        #     self, receiver="spec_in", name=f"{ch}_spectrogram_output_stream_sync"
                        # ),
                        # # no downstream condition, and we don't want one
                        cuda_stream_pool,
                        name=f"{ch}_spectrogram_output",
                        **spec_out_kwargs,
                    )
                    thread_pool.add(spectrogram_output, True)
                    self.add_flow(spectrogram, spectrogram_output)

        if ch_kwargs["pipeline"]["digital_rf"]:
            if (
                ch_kwargs["pipeline"]["converter"]
                and not ch_kwargs["pipeline"]["int_converter"]
            ):
                drf_sink = rf_array.DigitalRFSink_fc32(
                    self,
                    cuda_stream_pool,
                    name=f"{ch}_drf_sink",
                    **add_chunk_kwargs(last_chunk_shape, **ch_kwargs["drf_sink"]),
                )
                drf_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(drf_sink, True)
                self.add_flow(last_op, drf_sink)
            else:
                if ch_kwargs["pipeline"]["int_converter"]:
                    # due to check earlier, we know pipeline.converter is true here
                    # and the conversion back to int is needed
                    int_converter = rf_array.TypeConversionComplexFloatToInt(
                        self,
                        cuda_stream_pool,
                        name=f"{ch}_int_converter",
                    )
                    thread_pool.add(int_converter, True)
                    self.add_flow(last_op, int_converter)
                    last_op = int_converter

                drf_sink = rf_array.DigitalRFSink_sc16(
                    self,
                    cuda_stream_pool,
                    name=f"{ch}_drf_sink",
                    **add_chunk_kwargs(last_chunk_shape, **ch_kwargs["drf_sink"]),
                )
                drf_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(drf_sink, True)
                self.add_flow(last_op, drf_sink)

            if ch_kwargs["pipeline"]["metadata"]:
                dmd_sink = DigitalMetadataSink(
                    self,
                    name=f"{ch}_dmd_sink",
                    output_path=ch_kwargs["drf_sink"]["output_path"],
                    metadata_dir=f"{ch_kwargs['drf_sink']['channel_dir']}/metadata",
                    subdir_cadence_secs=ch_kwargs["drf_sink"]["subdir_cadence_secs"],
                    file_cadence_secs=ch_kwargs["drf_sink"]["file_cadence_millisecs"]
                    // 1000,
                    uuid=ch_kwargs["drf_sink"]["uuid"],
                    filename_prefix="metadata",
                    metadata=ch_kwargs.get("metadata", {}),
                )
                dmd_sink.spec.inputs["rf_in"].connector(
                    holoscan.core.IOSpec.ConnectorType.DOUBLE_BUFFER,
                    capacity=25,
                    policy=0,  # pop
                )
                thread_pool.add(dmd_sink, True)
                self.add_flow(last_op, dmd_sink)


def main():
    parser = build_config_parser()
    cfg = parser.parse_args()

    logger = logging.getLogger("holoscan.mep_recorder")

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
                skip_unset=True,
                skip_default=False,
                skip_link_targets=False,
            )
        )

    app = App([sys.executable, sys.argv[0]])
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
