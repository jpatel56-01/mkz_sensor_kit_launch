# mkz_sensor_kit_launch/launch/pointcloud_preprocessor.launch.py
#
# 3-LiDAR preprocessing pipeline
#
# TOP (Pandar64)   -> distortion -> ring filter
# LEFT (VLP16)     -> distortion -> crop box -> ring filter
# RIGHT (VLP16)    -> distortion -> crop box -> ring filter
#
# All three feed a real concatenate node:
# /sensing/lidar/concatenated/pointcloud

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

    distortion_param_file = ParameterFile(
        LaunchConfiguration("distortion_correction_node_param_path"),
        allow_substs=True,
    )

    ring_param_file = ParameterFile(
        LaunchConfiguration("ring_outlier_filter_node_param_path"),
        allow_substs=True,
    )

    cropbox_param_file = ParameterFile(
        LaunchConfiguration("crop_box_filter_param_path"),
        allow_substs=True,
    )

    concat_param_file = ParameterFile(
        LaunchConfiguration("concatenate_and_time_sync_node_param_path"),
        allow_substs=True,
    )

    nodes = []

    # ---------------- TOP LIDAR (Pandar64) ----------------
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="top_distortion_corrector",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "/sensing/pointcloud_raw_ex"),
                ("~/output/pointcloud", "top/rectified/pointcloud"),
            ],
            parameters=[distortion_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="top_ring_filter",
            remappings=[
                ("input", "top/rectified/pointcloud"),
                ("output", "top/filtered/pointcloud"),
            ],
            parameters=[ring_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # ---------------- LEFT VLP16 ----------------
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="left_distortion_corrector",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "/sensing/velodyne_left/velodyne_points"),
                ("~/output/pointcloud", "left/rectified/pointcloud"),
            ],
            parameters=[distortion_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="left_vehicle_self_cropbox",
            remappings=[
                ("input", "left/rectified/pointcloud"),
                ("output", "left/cropped/pointcloud"),
            ],
            parameters=[cropbox_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="left_ring_filter",
            remappings=[
                ("input", "left/cropped/pointcloud"),
                ("output", "left/filtered/pointcloud"),
            ],
            parameters=[ring_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # ---------------- RIGHT VLP16 ----------------
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="right_distortion_corrector",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "/sensing/velodyne_right/velodyne_points"),
                ("~/output/pointcloud", "right/rectified/pointcloud"),
            ],
            parameters=[distortion_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="right_vehicle_self_cropbox",
            remappings=[
                ("input", "right/rectified/pointcloud"),
                ("output", "right/cropped/pointcloud"),
            ],
            parameters=[cropbox_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="right_ring_filter",
            remappings=[
                ("input", "right/cropped/pointcloud"),
                ("output", "right/filtered/pointcloud"),
            ],
            parameters=[ring_param_file],
            extra_arguments=[{"use_intra_process_comms": use_intra_process}],
        )
    )

    # ---------------- CONCATENATE NODE ----------------
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::PointCloudConcatenateDataSynchronizerComponent",
            name="pointcloud_concatenator",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("output", "lidar/concatenated/pointcloud"),
                ("output_info", "lidar/concatenated/pointcloud_info"),
            ],
            parameters=[concat_param_file],
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
            DeclareLaunchArgument(
                "pointcloud_container_name",
                default_value="mkz_pointcloud_container",
            ),
            DeclareLaunchArgument(
                "container_namespace",
                default_value="/sensing",
            ),
            DeclareLaunchArgument(
                "use_intra_process",
                default_value="True",
            ),
            DeclareLaunchArgument(
                "distortion_correction_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "distortion_corrector_node.param.yaml"]
                ),
            ),
            DeclareLaunchArgument(
                "ring_outlier_filter_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "ring_outlier_filter_node.param.yaml"]
                ),
            ),
            DeclareLaunchArgument(
                "crop_box_filter_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "vehicle_self_cropbox.param.yaml"]
                ),
            ),
            DeclareLaunchArgument(
                "concatenate_and_time_sync_node_param_path",
                default_value=PathJoinSubstitution(
                    [pkg_share, "config", "concatenate_and_time_sync_node.param.yaml"]
                ),
            ),
            OpaqueFunction(function=_launch_setup),
        ]
    )
