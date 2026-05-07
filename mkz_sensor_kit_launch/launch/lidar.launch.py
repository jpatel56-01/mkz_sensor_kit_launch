# mkz_sensor_kit_launch/launch/lidar.launch.py

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare
from launch_ros.actions import ComposableNodeContainer


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    pointcloud_container_name = DeclareLaunchArgument(
        "pointcloud_container_name",
        default_value="mkz_pointcloud_container",
    )

    use_multithread = DeclareLaunchArgument(
        "use_multithread",
        default_value="True",
    )

    use_intra_process = DeclareLaunchArgument(
        "use_intra_process",
        default_value="True",
    )

    config_file = DeclareLaunchArgument(
        "config_file",
        default_value=PathJoinSubstitution([pkg, "config", "Pandar64.param.yaml"]),
    )

    distortion_correction_node_param_path = DeclareLaunchArgument(
        "distortion_correction_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "distortion_corrector_node.param.yaml"]
        ),
    )

    ring_outlier_filter_node_param_path = DeclareLaunchArgument(
        "ring_outlier_filter_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "ring_outlier_filter_node.param.yaml"]
        ),
    )

    concatenate_and_time_sync_node_param_path = DeclareLaunchArgument(
        "concatenate_and_time_sync_node_param_path",
        default_value=PathJoinSubstitution(
            [pkg, "config", "concatenate_and_time_sync_node.param.yaml"]
        ),
    )

    container = ComposableNodeContainer(
        name=LaunchConfiguration("pointcloud_container_name"),
        namespace="/sensing",
        package="rclcpp_components",
        executable="component_container_mt",
        composable_node_descriptions=[],
        output="screen",
        arguments=["--ros-args"],
    )

    include_hesai_driver = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "nebula_node_container.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "config_file": LaunchConfiguration("config_file"),
            "sensor_model": "Pandar64",
            "host_ip": "192.168.3.100",
            "sensor_ip": "192.168.3.104",
            "frame_id": "hesai_lidar",
            "data_port": "2368",
            "gnss_port": "10110",
            "return_mode": "Strongest",
            "rotation_speed_rpm": "1200",
            "packet_mtu_size": "1500",
            "udp_socket_receive_buffer_size_bytes": "5400000",
            "udp_only": "true",
        }.items(),
    )

    include_velodyne_left = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "nebula_node_container.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "config_file": PathJoinSubstitution([pkg, "config", "LVLP16.param.yaml"]),
            "sensor_model": "VLP16",
            "host_ip": "192.168.3.20",
            "sensor_ip": "192.168.3.201",
            "frame_id": "velodyne_left",
            "data_port": "2368",
            "gnss_port": "2369",
            "return_mode": "Dual",
            "rotation_speed_rpm": "1200",
            "packet_mtu_size": "1500",
            "udp_socket_receive_buffer_size_bytes": "5400000",
            "udp_only": "false",
        }.items(),
    )

    include_velodyne_right = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "nebula_node_container.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "config_file": PathJoinSubstitution([pkg, "config", "RVLP16.param.yaml"]),
            "sensor_model": "VLP16",
            "host_ip": "192.168.3.50",
            "sensor_ip": "192.168.3.245",
            "frame_id": "velodyne_right",
            "data_port": "2368",
            "gnss_port": "2369",
            "return_mode": "Dual",
            "rotation_speed_rpm": "1200",
            "packet_mtu_size": "1500",
            "udp_socket_receive_buffer_size_bytes": "5400000",
            "udp_only": "false",
        }.items(),
    )

    include_preproc_loader = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "pointcloud_preprocessor.launch.py"])
        ),
        launch_arguments={
            "pointcloud_container_name": LaunchConfiguration("pointcloud_container_name"),
            "container_namespace": "/sensing",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "distortion_correction_node_param_path": LaunchConfiguration(
                "distortion_correction_node_param_path"
            ),
            "ring_outlier_filter_node_param_path": LaunchConfiguration(
                "ring_outlier_filter_node_param_path"
            ),
            "concatenate_and_time_sync_node_param_path": LaunchConfiguration(
                "concatenate_and_time_sync_node_param_path"
            ),
        }.items(),
    )

    return LaunchDescription(
        [
            pointcloud_container_name,
            use_multithread,
            use_intra_process,
            config_file,
            distortion_correction_node_param_path,
            ring_outlier_filter_node_param_path,
            concatenate_and_time_sync_node_param_path,
            container,
            include_hesai_driver,
            include_velodyne_left,
            include_velodyne_right,
            include_preproc_loader,
        ]
    )
