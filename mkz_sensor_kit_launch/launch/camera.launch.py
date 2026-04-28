from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import ComposableNodeContainer
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    # Container configuration (mirrors lidar.launch.py)
    camera_container_name = DeclareLaunchArgument(
        "camera_container_name",
        default_value="mkz_camera_container",
        description="Name of the camera container created under /sensing",
    )

    use_intra_process = DeclareLaunchArgument(
        "use_intra_process",
        default_value="True",
        description="Enable intra-process comms for components",
    )
    
    # Pylon param file
    param_file = DeclareLaunchArgument(
        "param_file",
        default_value=PathJoinSubstitution([pkg, "config", "pylon_FP_param.yaml"]),
        description="Pylon camera YAML",
    )
    
    # 1) Create the camera container under /sensing
    container = ComposableNodeContainer(
        name=LaunchConfiguration("camera_container_name"),
        namespace="/sensing",
        package="rclcpp_components",
        executable="component_container_mt",
        composable_node_descriptions=[],
        output="screen",
        arguments=["--ros-args"],
    )
    
    # 2) Load pylon component into that container
    include_driver = IncludeLaunchDescription(
        PythonLaunchDescriptionSource(
            PathJoinSubstitution([pkg, "launch", "pylon_node_container.launch.py"])
        ),
        launch_arguments={
            "camera_container_name": LaunchConfiguration("camera_container_name"),
            "container_namespace": "/sensing",
            "use_intra_process": LaunchConfiguration("use_intra_process"),
            "param_file": LaunchConfiguration("param_file"),
            "node_name": "pylon_ros2_camera_node",
        }.items(),
    )

    return LaunchDescription([
        camera_container_name,
        use_intra_process,
        param_file,
        container,
        include_driver,
    ])
