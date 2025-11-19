# mkz_sensor_kit_launch/launch/pointcloud_preprocessor.launch.py
#
# Loads pointcloud preprocessor composable nodes into an existing container.
# The container FQN is computed from:
#   container_namespace + "/" + pointcloud_container_name
#
# Default target container: /sensing/mkz_pointcloud_container
#
# Pipeline (single top LiDAR):
#   nebula driver  -->  crop_box_filter
#                    --> distortion_corrector_node
#                    --> ring_outlier_filter
#                    --> (optional) concatenate_and_time_sync_node
#
# Final output is wired to /sensing/lidar/concatenated/pointcloud so
# downstream Autoware modules see the expected topic.

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
from launch_ros.substitutions import FindPackageShare


# Namespace for all preprocessor nodes
POINTCLOUD_NS = "/sensing/pointcloud_preprocessor"

# Existing topics from your setup
RAW_TOPIC = "/sensing/lidar/top/pointcloud_raw_ex"
CROP_OUTPUT_TOPIC = "crop_box_filtered/pointcloud_ex"
RECTIFIED_TOPIC = "rectified/pointcloud_ex"
OUTLIER_FILTERED_TOPIC = "outlier_filtered/pointcloud"

# Standard Autoware final lidar topic
FINAL_POINTCLOUD_TOPIC = "/sensing/lidar/concatenated/pointcloud"


def _build_nodes(context):
    use_intra_process = (
        LaunchConfiguration("use_intra_process").perform(context).lower() == "true"
    )
    use_concat_filter = (
        LaunchConfiguration("use_concat_filter").perform(context).lower() == "true"
    )

    # Parameter files (copied into mkz_sensor_kit_launch/config)
    crop_param = ParameterFile(
        LaunchConfiguration("crop_box_filter_node_param_path").perform(context),
        allow_substs=True,
    )
    distortion_param = ParameterFile(
        LaunchConfiguration("distortion_corrector_node_param_path").perform(context),
        allow_substs=True,
    )
    ring_param = ParameterFile(
        LaunchConfiguration("ring_outlier_filter_node_param_path").perform(context),
        allow_substs=True,
    )
    concat_param = ParameterFile(
        LaunchConfiguration("concatenate_and_time_sync_node_param_path").perform(context),
        allow_substs=True,
    )

    nodes = []

    # 1. Crop box filter: pointcloud_raw_ex -> crop_box_filtered/pointcloud_ex
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="crop_box_filter",
            namespace=POINTCLOUD_NS,
            remappings=[
                ("input", RAW_TOPIC),
                ("output", CROP_OUTPUT_TOPIC),
            ],
            parameters=[crop_param],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 2. Distortion corrector: crop_box_filtered -> rectified
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="distortion_corrector_node",
            namespace=POINTCLOUD_NS,
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", CROP_OUTPUT_TOPIC),
                ("~/output/pointcloud", RECTIFIED_TOPIC),
            ],
            parameters=[distortion_param],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 3. Ring outlier filter: rectified -> outlier_filtered
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="ring_outlier_filter",
            namespace=POINTCLOUD_NS,
            remappings=[
                ("input", RECTIFIED_TOPIC),
                ("output", OUTLIER_FILTERED_TOPIC),
            ],
            parameters=[ring_param],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 4. Concatenate + Time Sync (optional; default ON even for single LiDAR)
    if use_concat_filter:
        nodes.append(
            ComposableNode(
                package="autoware_pointcloud_preprocessor",
                plugin=(
                    "autoware::pointcloud_preprocessor::"
                    "PointCloudConcatenateDataSynchronizerComponent"
                ),
                name="concatenate_and_time_sync_node",
                namespace=POINTCLOUD_NS,
                # For single LiDAR: outlier_filtered -> /sensing/lidar/concatenated/pointcloud
                remappings=[
                    ("input", OUTLIER_FILTERED_TOPIC),
                    ("output", FINAL_POINTCLOUD_TOPIC),
                ],
                parameters=[concat_param],
                extra_arguments=[{"use_intra_process_comms": use_intra_process}],
            )
        )
    else:
        # If concat is disabled, still publish the outlier-filtered cloud
        # on /sensing/lidar/concatenated/pointcloud so downstream modules work.
        nodes.append(
            ComposableNode(
                package="autoware_pointcloud_preprocessor",
                plugin=(
                    "autoware::pointcloud_preprocessor::"
                    "PointCloudConcatenateDataSynchronizerComponent"
                ),
                name="passthrough_pointcloud",
                namespace=POINTCLOUD_NS,
                remappings=[
                    ("input", OUTLIER_FILTERED_TOPIC),
                    ("output", FINAL_POINTCLOUD_TOPIC),
                ],
                parameters=[concat_param],
                extra_arguments=[{"use_intra_process_comms": use_intra_process}],
            )
        )

    return nodes


def _launch_setup(context, *args, **kwargs):
    container_namespace = LaunchConfiguration("container_namespace").perform(context)
    container_name = LaunchConfiguration("pointcloud_container_name").perform(context)

    # Normalize and join to form the container FQN
    container_namespace = container_namespace.rstrip("/")
    container_name = container_name.lstrip("/")
    container_fqn = f"{container_namespace}/{container_name}"

    nodes = _build_nodes(context)

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
            # Container identity (must match lidar.launch.py / nebula setup)
            DeclareLaunchArgument(
                "pointcloud_container_name",
                default_value="mkz_pointcloud_container",
                description="Name of the pointcloud container node.",
            ),
            DeclareLaunchArgument(
                "container_namespace",
                default_value="/sensing",
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
                default_value="True",
                description="Enable concatenate/time-sync component.",
            ),
            # Params for crop box filter
            DeclareLaunchArgument(
                "crop_box_filter_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "crop_box_filter_node.param.yaml"]
                ),
                description="Parameter file for the crop box filter component.",
            ),
            # Params for distortion corrector
            DeclareLaunchArgument(
                "distortion_corrector_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "distortion_corrector_node.param.yaml"]
                ),
                description="Parameter file for the distortion corrector component.",
            ),
            # Params for ring outlier filter
            DeclareLaunchArgument(
                "ring_outlier_filter_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "ring_outlier_filter_node.param.yaml"]
                ),
                description="Parameter file for the ring outlier filter component.",
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

