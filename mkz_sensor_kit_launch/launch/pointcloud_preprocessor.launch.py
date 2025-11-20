import os
from ament_index_python.packages import get_package_share_directory
import launch
from launch.actions import DeclareLaunchArgument, OpaqueFunction, SetLaunchConfiguration
from launch.conditions import IfCondition, UnlessCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import LoadComposableNodes
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile

def launch_setup(context, *args, **kwargs):
    concat_param = ParameterFile(
        LaunchConfiguration("concatenate_and_time_sync_node_param_path").perform(context),
        allow_substs=True,
    )
    concat_component = ComposableNode(
        package="autoware_pointcloud_preprocessor",
        plugin="autoware::pointcloud_preprocessor::PointCloudConcatenateDataSynchronizerComponent",
        name="concatenate_data",
        remappings=[
            ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
            ("output", "concatenated/pointcloud"),
        ],
        parameters=[concat_param],
        extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
    )
    load_concat = LoadComposableNodes(
        composable_node_descriptions=[concat_component],
        target_container=LaunchConfiguration("pointcloud_container_name"),
        condition=IfCondition(LaunchConfiguration("use_concat_filter")),
    )
    return [load_concat]

def generate_launch_description():
    launch_arguments = []
    def add_arg(name, default=None):
        launch_arguments.append(DeclareLaunchArgument(name, default_value=default))

    mkz_share = get_package_share_directory("mkz_sensor_kit_launch")
    add_arg("base_frame", "base_link")
    add_arg("use_multithread", "False")
    add_arg("use_intra_process", "True")
    add_arg("pointcloud_container_name", "pointcloud_container")
    add_arg("use_concat_filter", "True")
    add_arg("concatenate_and_time_sync_node_param_path",
            os.path.join(mkz_share, "config", "concatenate_and_time_sync_node.param.yaml"))

    set_ce  = SetLaunchConfiguration("container_executable","component_container", condition=UnlessCondition(LaunchConfiguration("use_multithread")))
    set_cem = SetLaunchConfiguration("container_executable","component_container_mt", condition=IfCondition(LaunchConfiguration("use_multithread")))

    return launch.LaunchDescription(launch_arguments + [set_ce, set_cem, OpaqueFunction(function=launch_setup)])

