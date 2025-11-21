# mkz_sensor_kit_launch/launch/lidar.launch.py
#
# Top-level LiDAR launch for the MKZ sensor kit.
# - Creates ONE pointcloud container under /sensing/lidar
# - Loads the Nebula Hesai driver into that container
# - Loads the pointcloud preprocessor nodes into the same container

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare
from launch_ros.actions import ComposableNodeContainer


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    # Container configuration
    pointcloud_container_name = DeclareLaunchArgument(
        "pointcloud_container_name",
        default_value="mkz_pointcloud_container",
        description="Name of the ONE pointcloud container created under /sensing/lidar",
    )
    use_multithread = DeclareLaunchArgument(
        "use_multithread",
        default_value="True",
        description="(Reserved) Use multithreaded container; currently always mt",
    )
    use_intra_process = DeclareLaunchArgument(
        "use_intra_process",
        default_value="True",
        description="Enable intra-process comms for components",
    )

    # Driver package + config (Nebula Hesai)
    sensor_launch_pkg = DeclareLaunchArgument(
        "sensor_launch_pkg",
        default_value="nebula_ros",
        description="Package that provides HesaiRosWrapper component",
    )
    config_file = DeclareLaunchArgument(
        "config_file",
        default_value=PathJoinSubstitution(
            [pkg, "config", "Pandar64.param.yaml"]
        ),
        description="Nebula Hesai YAML (single source of truth for driver params)",
    )

    # Frames
    input_frame = DeclareLaunchArgument(
        "input_frame",
        default_value="base_link",
        description="Input frame for filters",
    )
    output_frame = DeclareLaunchArgument(
        "output_frame",
        default_value="base_link",
        description="Output frame for filters",
    )

    # Preprocessor parameter files (all under mkz_sensor_kit_launch/config)
    vehicle_mirror_param_file = DeclareLaunchArgument(
        "vehicle_mirror_param_file",
        default_value=PathJoinSubstitution(
            [pkg, "config", "vehicle_mirror.param.yaml"]
        ),
        description="YAML with mirror crop bounds; leave blank to disable",
    )
    distortion_correction_node_param_path = DeclareLaunchArgument(
        "distortion_correction_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "distortion_corrector_node.param.yaml"]
        ),
        description="Distortion corrector params",
    )
    ring_outlier_filter_node_param_path = DeclareLaunchArgument(
        "ring_outlier_filter_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "ring_outlier_filter_node.param.yaml"]
        ),
        description="Ring outlier filter params",
    )
    concatenate_and_time_sync_node_param_path = DeclareLaunchArgument(
        "concatenate_and_time_sync_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "concatenate_and_time_sync_node.param.yaml"]
        ),
        description="Concatenate/time-sync params",
    )
    use_concat_filter = DeclareLaunchArgument(
        "use_concat_filter",
        default_value="False",  # default off for single-LiDAR setup
        description="Enable concatenate/time-sync node",
    )

    # 1) Create the pointcloud container under /sensing/lidar
    container = ComposableNodeContainer(
        name=LaunchConfiguration("pointcloud_container_name"),
        namespace="/sensing/lidar",
        package="rclcpp_components",
        executable="component_container_mt",  # multithreaded
        composable_node_descriptions=[],
        output="screen",
        arguments=["--ros-args"],
    )

    # 2) Load Hesai (Nebula) driver into that container
    include_driver = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "nebula_node_container.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing/lidar",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "config_file": LaunchConfiguration("config_file"),
            # extra args are passed through for future use / consistency
            "sensor_model": "Pandar64",
            "host_ip": "192.168.3.100",
            "sensor_ip": "192.168.3.104",
            "frame_id": "hesai_lidar",
            "data_port": "2368",
            "gnss_port": "10110",
            "return_mode": "Strongest",
            "rotation_speed_rpm": "600",
            "packet_mtu_size": "1500",
            "udp_socket_receive_buffer_size_bytes": "5400000",
            "udp_only": "true",
        }.items(),
    )

    # 3) Load preprocessor nodes into the SAME container
    include_preproc_loader = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "pointcloud_preprocessor.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing/lidar",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "use_concat_filter": LaunchConfiguration("use_concat_filter"),
            "concatenate_and_time_sync_node_param_path": LaunchConfiguration(
                "concatenate_and_time_sync_node_param_path"
            ),
            "input_frame": LaunchConfiguration("input_frame"),
            "output_frame": LaunchConfiguration("output_frame"),
            "vehicle_mirror_param_file": LaunchConfiguration("vehicle_mirror_param_file"),
            "distortion_correction_node_param_path": LaunchConfiguration(
                "distortion_correction_node_param_path"
            ),
            "ring_outlier_filter_node_param_path": LaunchConfiguration(
                "ring_outlier_filter_node_param_path"
            ),
        }.items(),
    )

    return LaunchDescription(
        [
            pointcloud_container_name,
            use_multithread,
            use_intra_process,
            sensor_launch_pkg,
            config_file,
            input_frame,
            output_frame,
            vehicle_mirror_param_file,
            distortion_correction_node_param_path,
            ring_outlier_filter_node_param_path,
            concatenate_and_time_sync_node_param_path,
            use_concat_filter,
            container,
            include_driver,
            include_preproc_loader,
        ]
    )

