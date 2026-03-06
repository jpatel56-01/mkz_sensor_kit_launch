# mkz_sensor_kit_launch/launch/camera.launch.py
#
# Top-level camera launch for the MKZ sensor kit.
# - Creates ONE camera container under /sensing
# - Loads the Basler pylon component into that container
# - Publishes Autoware-convention topics under /sensing/camera/camera0/*
# - Runs image_proc to debayer+rectify -> /sensing/camera/camera0/image_rect_color

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare
from launch_ros.actions import ComposableNodeContainer, Node


def generate_launch_description():
    pkg = FindPackageShare("mkz_sensor_kit_launch")

    # Container configuration (mirrors lidar.launch.py)
    camera_container_name = DeclareLaunchArgument(
        "camera_container_name",
        default_value="mkz_camera_container",
        description="Name of the ONE camera container created under /sensing",
    )
    use_intra_process = DeclareLaunchArgument(
        "use_intra_process",
        default_value="True",
        description="Enable intra-process comms for components",
    )

    # Pylon param file
    param_file = DeclareLaunchArgument(
        "param_file",
        default_value=PathJoinSubstitution([pkg, "config", "pylon_FW_param.yaml"]),
        description="Pylon camera YAML (single source of truth for driver params)",
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

    # 3) Run image_proc in the Autoware namespace:
    # - subscribes to image_raw + camera_info
    # - debayers (for Bayer cameras) + rectifies
    # - publishes image_rect_color in the same namespace
    #
    # This is exactly what the image_proc docs describe (debayer + undistort/rectify,
    # output image_rect_color). :contentReference[oaicite:3]{index=3}
    debayer = Node(
    	package="image_proc",
        executable="debayer_node",
    	name="debayer",
    	namespace="/sensing/camera/camera0",
    	output="screen",
    	# input: image_raw (Bayer) -> output: image_color (BGR) and image_mono
    )

    rectify_color = Node(
    	package="image_proc",
    	executable="rectify_node",
    	name="rectify_color",
    	namespace="/sensing/camera/camera0",
    	output="screen",
    	remappings=[
            # RectifyNode subscribes to "image" + "camera_info" :contentReference[oaicite:6]{index=6}
            ("image", "image_color"),
            # Output "image_rect" -> rename to "image_rect_color"
            ("image_rect", "image_rect_color"),
        ],
    )

    return LaunchDescription(
        [
            camera_container_name,
            use_intra_process,
            param_file,
            container,
            include_driver,
            debayer,
            rectify_color,
        ]
    )
