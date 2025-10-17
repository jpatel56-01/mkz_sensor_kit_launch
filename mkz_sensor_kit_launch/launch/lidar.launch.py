from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription, OpaqueFunction, SetLaunchConfiguration
from launch.conditions import IfCondition, UnlessCondition
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare

def generate_launch_description():
    mkz_share = FindPackageShare('mkz_sensor_kit_launch')

    # ---- Args (names aligned to your preference) ----
    sensor_model        = LaunchConfiguration('sensor_model')
    launch_hw           = LaunchConfiguration('launch_hw')
    nebula_config_file  = LaunchConfiguration('nebula_config_file')  # <- your name
    sensor_namespace    = LaunchConfiguration('sensor_namespace')
    sensor_frame        = LaunchConfiguration('sensor_frame')
    host_ip             = LaunchConfiguration('host_ip')
    pointcloud_container_name = LaunchConfiguration('pointcloud_container_name')
    use_intra_process   = LaunchConfiguration('use_intra_process')
    use_multithread     = LaunchConfiguration('use_multithread')
    use_concat_filter   = LaunchConfiguration('use_concat_filter')

    return LaunchDescription([
        # Defaults
        DeclareLaunchArgument('sensor_model', default_value='Pandar64'),
        DeclareLaunchArgument('launch_hw', default_value='true'),
        DeclareLaunchArgument('sensor_namespace', default_value='top'),
        DeclareLaunchArgument('sensor_frame', default_value='hesai_lidar'),
        DeclareLaunchArgument('host_ip', default_value='192.168.3.100'),
        DeclareLaunchArgument('nebula_config_file',
            default_value=PathJoinSubstitution([mkz_share, 'config', 'Pandar64.param.yaml'])),
        DeclareLaunchArgument('pointcloud_container_name', default_value='pointcloud_container'),
        DeclareLaunchArgument('use_intra_process', default_value='true'),
        DeclareLaunchArgument('use_multithread', default_value='true'),
        DeclareLaunchArgument('use_concat_filter', default_value='true'),

        # --- Nebula + filter chain (your container) ---
        IncludeLaunchDescription(
            PythonLaunchDescriptionSource(
                PathJoinSubstitution([mkz_share, 'launch', 'nebula_node_container.launch.py'])
            ),
            launch_arguments={
                'sensor_model': sensor_model,
                'launch_driver': launch_hw,
                'host_ip': host_ip,
                'frame_id': sensor_frame,
                # keep your arg name, pass through to the container:
                'config_file': nebula_config_file,                 # container expects 'config_file'
                'pointcloud_container_name': pointcloud_container_name,
                'use_intra_process': use_intra_process,
                'use_multithread': use_multithread,
                'lidar_container_name': pointcloud_container_name, # backward-compat in container
                # local param files in mkz/config
                'distortion_correction_node_param_path': PathJoinSubstitution([mkz_share, 'config', 'distortion_corrector_node.param.yaml']),
                'ring_outlier_filter_node_param_path': PathJoinSubstitution([mkz_share, 'config', 'ring_outlier_filter_node.param.yaml']),
            }.items()
        ),

        # --- Global preprocessor (concat off for single lidar) ---
        IncludeLaunchDescription(
            PythonLaunchDescriptionSource(
                PathJoinSubstitution([mkz_share, 'launch', 'pointcloud_preprocessor.launch.py'])
            ),
            launch_arguments={
                'pointcloud_container_name': pointcloud_container_name,
                'use_intra_process': use_intra_process,
                'use_multithread': use_multithread,
                'use_concat_filter': use_concat_filter,
            }.items()
        ),
    ])

