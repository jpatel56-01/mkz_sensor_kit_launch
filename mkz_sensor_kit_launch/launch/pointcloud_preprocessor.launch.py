# mkz_sensor_kit_launch/launch/pointcloud_preprocessor.launch.py
#
# Loads LiDAR preprocessing composable nodes into an existing container.
# The container FQN is computed from:
#   container_namespace + "/" + pointcloud_container_name
#
# Default target container: /sensing/lidar/mkz_pointcloud_container
#
# Simplified LiDAR pipeline (single LiDAR, no self/mirror crops):
#   pointcloud_raw_ex  -->  rectified/pointcloud_ex  -->  pointcloud
#                                                       ↘  concatenated/pointcloud
#
# Global topics (with container namespace /sensing/lidar):
#   /sensing/lidar/pointcloud_raw_ex
#   /sensing/lidar/rectified/pointcloud_ex
#   /sensing/lidar/pointcloud
#   /sensing/lidar/concatenated/pointcloud

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
from launch_ros.substitutions import FindPackageShare


def _build_nodes(context):
    use_intra_process = (
        LaunchConfiguration("use_intra_process").perform(context).lower() == "true"
    )
    # Kept for compatibility but NOT used (we no longer spawn the real concat filter)
    _ = LaunchConfiguration("use_concat_filter").perform(context)

    # Distortion & ring-outlier param files (from lidar.launch.py)
    distortion_param_file = ParameterFile(
        LaunchConfiguration("distortion_correction_node_param_path"), allow_substs=True
    )
    ring_param_file = ParameterFile(
        LaunchConfiguration("ring_outlier_filter_node_param_path"), allow_substs=True
    )

    input_frame = LaunchConfiguration("input_frame").perform(context)
    output_frame = LaunchConfiguration("output_frame").perform(context)

    nodes = []

    # 1) Distortion corrector
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="distortion_corrector_node",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "pointcloud_raw_ex"),
                ("~/output/pointcloud", "rectified/pointcloud_ex"),
            ],
            parameters=[
                distortion_param_file,
                {"processing_time_threshold_sec": 0.5},
            ],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 2) Ring outlier filter (main LiDAR output for Autoware)
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="ring_outlier_filter_node",
            remappings=[
                ("input", "rectified/pointcloud_ex"),
                ("output", "pointcloud"),
            ],
            parameters=[
                ring_param_file,
                {"processing_time_threshold_sec": 0.5},
            ],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # 3) "Fake concat" passthrough so you have BOTH topics:
    #    - /sensing/lidar/pointcloud
    #    - /sensing/lidar/concatenated/pointcloud  (identical cloud)
    #
    # We use a CropBoxFilterComponent with negative=False and huge bounds so it
    # simply republishes the same cloud under another topic name.
    concat_passthrough_params = {
        "input_frame": input_frame,
        "output_frame": output_frame,
        "negative": False,
        "min_x": -1000.0,
        "max_x": 1000.0,
        "min_y": -1000.0,
        "max_y": 1000.0,
        "min_z": -100.0,
        "max_z": 100.0,
    }

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="concat_passthrough",
            remappings=[
                ("input", "pointcloud"),
                ("output", "concatenated/pointcloud"),
            ],
            parameters=[
                concat_passthrough_params,
                {"processing_time_threshold_sec": 0.5},
            ],
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
            # Kept for compatibility; the real concat filter is not used anymore
            DeclareLaunchArgument(
                "use_concat_filter",
                default_value="False",
                description="(Unused) Enable concatenate/time-sync component.",
            ),
            # Frames (used by the passthrough CropBox)
            DeclareLaunchArgument(
                "input_frame",
                default_value="base_link",
                description="Input frame for passthrough concat filter.",
            ),
            DeclareLaunchArgument(
                "output_frame",
                default_value="base_link",
                description="Output frame for passthrough concat filter.",
            ),
            # Mirror crop YAML arg kept for compatibility (unused now)
            DeclareLaunchArgument(
                "vehicle_mirror_param_file",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "vehicle_mirror.param.yaml"]
                ),
                description="(Unused) Vehicle mirror crop YAML.",
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
            # Params for real concatenate/time-sync (kept for API compatibility; unused)
            DeclareLaunchArgument(
                "concatenate_and_time_sync_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "concatenate_and_time_sync_node.param.yaml"]
                ),
                description="(Unused) Parameter file for concatenate/time-sync component.",
            ),
            OpaqueFunction(function=_launch_setup),
        ]
    )

