from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import Node
from launch_ros.substitutions import FindPackageShare


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    param_file_camera0 = DeclareLaunchArgument(
        "param_file_camera0",
        default_value=PathJoinSubstitution([pkg, "config", "pylon_camera0_param.yaml"]),
        description="Pylon camera0 (front passenger) YAML",
    )

    param_file_camera1 = DeclareLaunchArgument(
        "param_file_camera1",
        default_value=PathJoinSubstitution([pkg, "config", "pylon_camera1_param.yaml"]),
        description="Pylon camera1 (front driver) YAML",
    )

    pylon_node_camera0 = Node(
        package="pylon_ros2_camera_wrapper",
        executable="pylon_ros2_camera_wrapper",
        name="pylon_ros2_camera_node",
        namespace="/sensing/camera0",          # namespace does the disambiguating
        output="screen",
        parameters=[LaunchConfiguration("param_file_camera0")],
        remappings=[
            ("pylon_ros2_camera_node/image_raw",   "/sensing/camera/camera0/image_raw"),
            ("pylon_ros2_camera_node/camera_info", "/sensing/camera/camera0/camera_info"),
        ],
    )

    pylon_node_camera1 = Node(
        package="pylon_ros2_camera_wrapper",
        executable="pylon_ros2_camera_wrapper",
        name="pylon_ros2_camera_node",
        namespace="/sensing/camera1",          # different namespace, same node name — clean
        output="screen",
        parameters=[LaunchConfiguration("param_file_camera1")],
        remappings=[
            ("pylon_ros2_camera_node/image_raw",   "/sensing/camera/camera1/image_raw"),
            ("pylon_ros2_camera_node/camera_info", "/sensing/camera/camera1/camera_info"),
        ],
    )

    return LaunchDescription([
        param_file_camera0,
        param_file_camera1,
        pylon_node_camera0,
        pylon_node_camera1,
    ])
