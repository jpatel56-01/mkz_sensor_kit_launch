# mkz_sensor_kit_launch/launch/nebula_node_container.launch.py
#
# Load the Hesai (Nebula) driver INTO the pointcloud container created by
# lidar.launch.py. The container FQN is computed from:
#   container_namespace + "/" + pointcloud_container_name
#
# Publishes (after remap) to: <container_ns>/pointcloud_raw_ex
# e.g. /sensing/lidar/pointcloud_raw_ex

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration, PathJoinSubstitution
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
from launch_ros.substitutions import FindPackageShare


def _load_into_existing_container(context):
    # Resolve container name + namespace and build FQN
    container_name = LaunchConfiguration("pointcloud_container_name").perform(context)
    container_ns = LaunchConfiguration("container_namespace").perform(context)
    if container_ns.endswith("/"):
        container_fqn = f"{container_ns}{container_name}"
    else:
        container_fqn = f"{container_ns}/{container_name}"

    use_ipc = LaunchConfiguration("use_intra_process").perform(context).lower() == "true"

    # Nebula YAML (contains most strongly-typed params)
    param_file = ParameterFile(LaunchConfiguration("config_file"), allow_substs=True)

    # Optional/overridable params
    sensor_model = LaunchConfiguration("sensor_model")
    host_ip = LaunchConfiguration("host_ip")
    sensor_ip = LaunchConfiguration("sensor_ip")
    frame_id = LaunchConfiguration("frame_id")
    data_port = LaunchConfiguration("data_port")
    gnss_port = LaunchConfiguration("gnss_port")
    return_mode = LaunchConfiguration("return_mode")
    rotation_rpm = LaunchConfiguration("rotation_speed_rpm")
    mtu = LaunchConfiguration("packet_mtu_size")
    rcvbuf = LaunchConfiguration("udp_socket_receive_buffer_size_bytes")
    udp_only = LaunchConfiguration("udp_only")

    driver = ComposableNode(
        package="nebula_ros",
        plugin="HesaiRosWrapper",
        name="hesai_ros_wrapper_node",
        # No explicit namespace here; topics are relative to the container namespace
        remappings=[
            ("pandar_points", "pointcloud_raw_ex"),
        ],
        parameters=[
            param_file,
            {"sensor_model": sensor_model},
            {"host_ip": host_ip},
            {"sensor_ip": sensor_ip},
            {"frame_id": frame_id},
            {"data_port": data_port},
            {"gnss_port": gnss_port},
            {"return_mode": return_mode},
            {"rotation_speed": rotation_rpm},
            {"packet_mtu_size": mtu},
            {"udp_socket_receive_buffer_size_bytes": rcvbuf},
            {"udp_only": udp_only},
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
            # Container identity (comes from lidar.launch.py)
            DeclareLaunchArgument(
                "pointcloud_container_name",
                default_value="mkz_pointcloud_container",
                description="Name of the pointcloud container node",
            ),
            DeclareLaunchArgument(
                "container_namespace",
                default_value="/sensing/lidar",
                description="Namespace of the pointcloud container node",
            ),
            # Nebula YAML (single source of truth, now in mkz_sensor_kit_launch/config)
            DeclareLaunchArgument(
                "config_file",
                default_value=PathJoinSubstitution(
                    [pkg, "config", "Pandar64.param.yaml"]
                ),
                description="Nebula driver params YAML",
            ),
            # Required/commonly changed params
            DeclareLaunchArgument("sensor_model", default_value="Pandar64"),
            DeclareLaunchArgument("host_ip", default_value="192.168.3.100"),
            DeclareLaunchArgument("sensor_ip", default_value="192.168.3.104"),
            DeclareLaunchArgument("frame_id", default_value="hesai_lidar"),
            DeclareLaunchArgument("data_port", default_value="2368"),
            DeclareLaunchArgument("gnss_port", default_value="10110"),
            DeclareLaunchArgument("return_mode", default_value="Strongest"),
            DeclareLaunchArgument("rotation_speed_rpm", default_value="600"),
            DeclareLaunchArgument("packet_mtu_size", default_value="1500"),
            DeclareLaunchArgument(
                "udp_socket_receive_buffer_size_bytes", default_value="5400000"
            ),
            DeclareLaunchArgument("udp_only", default_value="true"),
            # IPC on for composables
            DeclareLaunchArgument("use_intra_process", default_value="True"),
            OpaqueFunction(function=_load_into_existing_container),
        ]
    )

