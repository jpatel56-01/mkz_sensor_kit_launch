# mkz_sensor_kit_launch/launch/pointcloud_preprocessor.launch.py
#
# Loads pointcloud preprocessor composable nodes into an existing container.
# The container FQN is computed from:
#   container_namespace + "/" + pointcloud_container_name
#
# Default target container: /sensing/mkz_pointcloud_container

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
    use_concat_filter = (
        LaunchConfiguration("use_concat_filter").perform(context).lower() == "true"
    )

    concat_param_path = LaunchConfiguration(
        "concatenate_and_time_sync_node_param_path"
    ).perform(context)
    concat_param = ParameterFile(concat_param_path, allow_substs=True)

    nodes = []

    # Concatenate + Time Sync (optional; default disabled for single-LiDAR)
    if use_concat_filter:
        nodes.append(
            ComposableNode(
                package="autoware_pointcloud_preprocessor",
                plugin="autoware::pointcloud_preprocessor::PointCloudConcatenateDataSynchronizerComponent",
                name="concatenate_and_time_sync_node",
                namespace="/sensing/pointcloud_preprocessor",
                # For now this is a single-lidar pass-through:
                #   input:  /sensing/lidar/top/pointcloud_raw_ex
                #   output: /sensing/pointcloud
                # We can refine remappings later to match the full Autoware pipeline.
                remappings=[
                    ("input", "/sensing/lidar/top/pointcloud_raw_ex"),
                    ("output", "/sensing/pointcloud"),
                ],
                parameters=[concat_param],
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
                default_value="False",
                description="Enable concatenate/time-sync component.",
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

