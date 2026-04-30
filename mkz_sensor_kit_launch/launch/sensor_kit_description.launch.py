from launch import LaunchDescription
from launch_ros.actions import Node
from launch.substitutions import Command, FindExecutable, PathJoinSubstitution
from launch_ros.substitutions import FindPackageShare

def generate_launch_description():
    # Paths
    xacro_exe = FindExecutable(name="xacro")
    mkz_desc_pkg = FindPackageShare("mkz_sensor_kit_description")
    xacro_file = PathJoinSubstitution([mkz_desc_pkg, "urdf", "sensors.xacro"])
    config_dir = PathJoinSubstitution([mkz_desc_pkg, "config"])

    # Run xacro to generate URDF
    robot_description = Command([
        xacro_exe, " --inorder ", xacro_file,
        " config_dir:=", config_dir
    ])

    # Robot State Publisher Node
    state_pub_node = Node(
        package="robot_state_publisher",
        executable="robot_state_publisher",
        name="sensor_kit_state_publisher",
        output="screen",
        parameters=[{"robot_description": robot_description}]
    )

    return LaunchDescription([state_pub_node])

