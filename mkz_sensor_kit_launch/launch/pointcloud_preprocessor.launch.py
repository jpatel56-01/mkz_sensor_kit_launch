# mkz_sensor_kit_launch/launch/pointcloud_preprocessor.launch.py
#
# Loads LiDAR preprocessing composable nodes into an existing container.
# The container FQN is computed from:
#   container_namespace + "/" + pointcloud_container_name
#
# Default target container: /sensing/lidar/mkz_pointcloud_container
#
# Topic flow inside the container namespace (single LiDAR):
#   pandar_points              -> pointcloud_raw_ex          (from driver)
#   pointcloud_raw_ex          -> self_cropped/pointcloud_ex
#   self_cropped/pointcloud_ex -> mirror_cropped/pointcloud_ex
#   mirror_cropped/pointcloud_ex -> rectified/pointcloud_ex
#   rectified/pointcloud_ex    -> pointcloud   (final filtered cloud)
#
# If Autoware nodes expect /sensing/lidar/concatenated/pointcloud,
# you can use *subscriber-side* remap in those nodes:
#   <remap from="/sensing/lidar/concatenated/pointcloud"
#          to="/sensing/lidar/pointcloud"/>

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
from launch_ros.substitutions import FindPackageShare

import os
import yaml


def _build_nodes(context):
    use_intra_process = (
        LaunchConfiguration("use_intra_process").perform(context).lower() == "true"
    )
    use_concat_filter = (
        LaunchConfiguration("use_concat_filter").perform(context).lower() == "true"
    )

    input_frame = LaunchConfiguration("input_frame").perform(context)
    output_frame = LaunchConfiguration("output_frame").perform(context)

    # --- Mirror param YAML (optional) ---
    mirror_yaml_path = LaunchConfiguration("vehicle_mirror_param_file").perform(context)
    mirror_params_raw = {}
    if mirror_yaml_path and os.path.exists(mirror_yaml_path):
        try:
            with open(mirror_yaml_path, "r") as f:
                mirror_params_raw = yaml.safe_load(f) or {}
            if "ros__parameters" in mirror_params_raw:
                mirror_params_raw = mirror_params_raw["ros__parameters"]
        except Exception:
            mirror_params_raw = {}

    # Fallback to passthrough if nothing is set in YAML
    mirror_params = {
        "min_longitudinal_offset": mirror_params_raw.get(
            "min_longitudinal_offset", -1000.0
        ),
        "max_longitudinal_offset": mirror_params_raw.get(
            "max_longitudinal_offset", 1000.0
        ),
        "min_lateral_offset": mirror_params_raw.get("min_lateral_offset", -1000.0),
        "max_lateral_offset": mirror_params_raw.get("max_lateral_offset", 1000.0),
        "min_height_offset": mirror_params_raw.get("min_height_offset", -100.0),
        "max_height_offset": mirror_params_raw.get("max_height_offset", 100.0),
    }

    # --- Distortion & ring-outlier param files (from lidar.launch.py) ---
    distortion_param_file = ParameterFile(
        LaunchConfiguration("distortion_correction_node_param_path"), allow_substs=True
    )
    ring_param_file = ParameterFile(
        LaunchConfiguration("ring_outlier_filter_node_param_path"), allow_substs=True
    )

    nodes = []

    # 1) CropBox (self vehicle body)
    crop_self_params = {
        "input_frame": input_frame,
        "output_frame": output_frame,
        "negative": False,
        # Placeholder wide bounds; adjust to your MKZ body if needed
        "min_x": -100.0,
        "max_x": 100.0,
        "min_y": -100.0,
        "max_y": 100.0,
        "min_z": -10.0,
        "max_z": 10.0,
    }
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="crop_box_filter_self",
            remappings=[("input", "pointcloud_raw_ex"), ("output", "self_cropped/pointcloud_ex")],
            parameters=[crop_self_params, {"processing_time_threshold_sec": 0.5}],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 2) CropBox (mirror region), falls back to passthrough bounds if no mirror params are set
    crop_mirror_params = {
        "input_frame": input_frame,
        "output_frame": output_frame,
        "negative": True,
        "min_x": mirror_params["min_longitudinal_offset"],
        "max_x": mirror_params["max_longitudinal_offset"],
        "min_y": mirror_params["min_lateral_offset"],
        "max_y": mirror_params["max_lateral_offset"],
        "min_z": mirror_params["min_height_offset"],
        "max_z": mirror_params["max_height_offset"],
    }
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="crop_box_filter_mirror",
            remappings=[
                ("input", "self_cropped/pointcloud_ex"),
                ("output", "mirror_cropped/pointcloud_ex"),
            ],
            parameters=[crop_mirror_params, {"processing_time_threshold_sec": 0.5}],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 3) Distortion corrector
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="distortion_corrector_node",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "mirror_cropped/pointcloud_ex"),
                ("~/output/pointcloud", "rectified/pointcloud_ex"),
            ],
            parameters=[distortion_param_file, {"processing_time_threshold_sec": 0.5}],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 4) Ring outlier filter
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="ring_outlier_filter_node",
            remappings=[("input", "rectified/pointcloud_ex"), ("output", "pointcloud")],
            parameters=[ring_param_file, {"processing_time_threshold_sec": 0.5}],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 5) Optional concatenate + time sync (not needed for single LiDAR; default off)
    if use_concat_filter:
        concat_param_path = LaunchConfiguration(
            "concatenate_and_time_sync_node_param_path"
        ).perform(context)
        concat_param = ParameterFile(concat_param_path, allow_substs=True)

        nodes.append(
            ComposableNode(
                package="autoware_pointcloud_preprocessor",
                plugin="autoware::pointcloud_preprocessor::PointCloudConcatenateDataSynchronizerComponent",
                name="concatenate_and_time_sync_node",
                # Inside container ns, this becomes /sensing/lidar/concatenated/pointcloud
                remappings=[
                    ("input", "pointcloud"),
                    ("output", "concatenated/pointcloud"),
                ],
                parameters=[concat_param, {"processing_time_threshold_sec": 0.5}],
                extra_arguments=[{"use_intra_process_comms": use_intra_process}],
            )
        )

    return nodes


def _launch_setup(context, *args, **kwargs):
    container_name = LaunchConfiguration("pointcloud_container_name").perform(context)
    container_ns = LaunchConfiguration("container_namespace").perform(context)
    if container_ns.endswith("/"):
        container_fqn = f"{container_ns}{container_name}"
    else:
        container_fqn = f"{container_ns}/{container_name}"

    nodes = _build_nodes(context)

    if not nodes:
        # Nothing to load; return empty list cleanly
        return []

    return [
        LoadComposableNodes(
            target_container=container_fqn,
            composable_node_descriptions=nodes,
        )
    ]


def generate_launch_description():
    pkg_share = FindPackageShare("mkz_sensor_kit_launch")

    return LaunchDescription(
        [
            # Container identity (must match lidar.launch.py / nebula_node_container.launch.py)
            DeclareLaunchArgument(
                "pointcloud_container_name",
                default_value="mkz_pointcloud_container",
                description="Name of the pointcloud container node.",
            ),
            DeclareLaunchArgument(
                "container_namespace",
                default_value="/sensing/lidar",
                description="Namespace of the pointcloud container node.",
            ),
            # IPC toggle (recommended True for component containers)
            DeclareLaunchArgument(
                "use_intra_process",
                default_value="True",
                description="Enable intra-process comms for composable nodes.",
            ),
            # Whether to load the concatenate/time-sync node
            DeclareLaunchArgument(
                "use_concat_filter",
                default_value="False",
                description="Enable concatenate/time-sync component.",
            ),
            # Frames
            DeclareLaunchArgument(
                "input_frame",
                default_value="base_link",
                description="Input frame for crop filters.",
            ),
            DeclareLaunchArgument(
                "output_frame",
                default_value="base_link",
                description="Output frame for crop filters.",
            ),
            # Mirror crop YAML (optional)
            DeclareLaunchArgument(
                "vehicle_mirror_param_file",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "vehicle_mirror.param.yaml"]
                ),
                description="Vehicle mirror crop YAML; leave blank to disable.",
            ),
            # Distortion & ring filter param files
            DeclareLaunchArgument(
                "distortion_correction_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "distortion_corrector_node.param.yaml"]
                ),
                description="Parameter file for distortion corrector.",
            ),
            DeclareLaunchArgument(
                "ring_outlier_filter_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "ring_outlier_filter_node.param.yaml"]
                ),
                description="Parameter file for ring outlier filter.",
            ),
            # Params for concatenate/time-sync
            DeclareLaunchArgument(
                "concatenate_and_time_sync_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "concatenate_and_time_sync_node.param.yaml"]
                ),
                description="Parameter file for the concatenate/time-sync component.",
            ),
            OpaqueFunction(function=_launch_setup),
        ]
    )

