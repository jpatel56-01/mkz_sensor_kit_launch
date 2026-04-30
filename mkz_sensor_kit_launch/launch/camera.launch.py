from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    param_file = DeclareLaunchArgument(
        "param_file",
        default_value=PathJoinSubstitution([pkg, "config", "pylon_FP_param.yaml"]),
        description="Pylon camera YAML",
    )

    pylon_node = Node(
        package="pylon_ros2_camera_wrapper",
        executable="pylon_ros2_camera_wrapper",
        name="pylon_ros2_camera_node",
        namespace="/sensing",
        output="screen",
        parameters=[LaunchConfiguration("param_file")],
        remappings=[
            ("pylon_ros2_camera_node/image_raw", "/sensing/camera/camera0/image_raw"),
            ("pylon_ros2_camera_node/camera_info", "/sensing/camera/camera0/camera_info"),
        ],
    )

    return LaunchDescription([
        param_file,
        pylon_node,
    ])
