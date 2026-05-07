# mkz_sensor_kit_launch/launch/pylon_node_container.launch.py
#
# Load the Basler pylon ROS2 camera component INTO an existing container.
# Mirrors nebula_node_container.launch.py.

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
from launch_ros.substitutions import FindPackageShare


def _load_into_existing_container(context):
    container_name = LaunchConfiguration("camera_container_name").perform(context)
    container_ns = LaunchConfiguration("container_namespace").perform(context)

    if container_ns.endswith("/"):
        container_fqn = f"{container_ns}{container_name}"
    else:
        container_fqn = f"{container_ns}/{container_name}"

    use_ipc = LaunchConfiguration("use_intra_process").perform(context).lower() == "true"

    param_file = ParameterFile(LaunchConfiguration("param_file"), allow_substs=True)

    # Basler docs: wrapper launches the main component
    # `pylon_ros2_camera::PylonROS2CameraNode` from `pylon_ros2_camera_component`.
    # See basler/pylon-ros-camera README. (cited in chat response)
    driver = ComposableNode(
        package="pylon_ros2_camera_component",
        plugin="pylon_ros2_camera::PylonROS2CameraNode",
        name=LaunchConfiguration("node_name"),
        parameters=[param_file],
        # Remap the *actual* topics your node is publishing today:
        #   /pylon_ros2_camera_node/image_raw
        #   /pylon_ros2_camera_node/camera_info
        #   /pylon_ros2_camera_node/image_rect
        # …into Autoware convention under /sensing/camera/camera0/*
        remappings=[
            ("pylon_ros2_camera_node/image_raw", "/sensing/camera/camera0/image_raw"),
            ("pylon_ros2_camera_node/camera_info", "/sensing/camera/camera0/camera_info"),
        ],
        extra_arguments=[{"use_intra_process_comms": use_ipc}],
    )

    return [
        LoadComposableNodes(
            target_container=container_fqn,
            composable_node_descriptions=[driver],
        )
    ]


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "camera_container_name",
                default_value="mkz_camera_container",
                description="Name of the camera container node",
            ),
            DeclareLaunchArgument(
                "container_namespace",
                default_value="/sensing",
                description="Namespace of the camera container node",
            ),
            DeclareLaunchArgument(
                "param_file",
                default_value=PathJoinSubstitution([pkg, "config", "pylon_camera0_param.yaml"]),
                description="Pylon driver params YAML (your calibrated config)",
            ),
            DeclareLaunchArgument(
                "node_name",
                default_value="pylon_ros2_camera_node",
                description="Name of the pylon camera node inside the container",
            ),
            DeclareLaunchArgument("use_intra_process", default_value="True"),
            OpaqueFunction(function=_load_into_existing_container),
        ]
    )

