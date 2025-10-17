import os
from ament_index_python.packages import get_package_share_directory
import launch
from launch.actions import DeclareLaunchArgument, OpaqueFunction, SetLaunchConfiguration
from launch.conditions import IfCondition, UnlessCondition
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import ComposableNodeContainer
from launch_ros.descriptions import ComposableNode
from launch_ros.parameter_descriptions import ParameterFile
import yaml

def get_lidar_make(sensor_name):
    if sensor_name[:6].lower() == "pandar":
        return "Hesai", ".csv"
    elif sensor_name[:3].lower() in ["hdl", "vlp", "vls"]:
        return "Velodyne", ".yaml"
    elif sensor_name.lower() in ["helios", "bpearl"]:
        return "Robosense", None
    return "unrecognized_sensor_model"

def launch_setup(context, *args, **kwargs):
    def pdict(*keys):
        return {k: LaunchConfiguration(k) for k in keys}

    sensor_model = LaunchConfiguration("sensor_model").perform(context)
    sensor_make, sensor_ext = get_lidar_make(sensor_model)

    # Vehicle mirror params are optional; if unset, skip cropbox mirror shaping
    vehicle_mirror_param_path = LaunchConfiguration("vehicle_mirror_param_file").perform(context)
    mirror_params = {}
    if vehicle_mirror_param_path:
        with open(vehicle_mirror_param_path, "r") as f:
            mirror_params = yaml.safe_load(f).get("/**", {}).get("ros__parameters", {})

    # Local (mkz) param files
    mkz_share = get_package_share_directory("mkz_sensor_kit_launch")
    distortion_param = ParameterFile(
        LaunchConfiguration("distortion_correction_node_param_path").perform(context), allow_substs=True
    )
    ring_outlier_param = ParameterFile(
        LaunchConfiguration("ring_outlier_filter_node_param_path").perform(context), allow_substs=True
    )

    nodes = []

    # Nebula RosWrapper (driver)
    nebula_params = {
        "sensor_model": sensor_model,
        "launch_hw": LaunchConfiguration("launch_driver"),
        # pass your YAML (from top-level arg 'nebula_config_file') into the container as 'config_file'
        "config_file": LaunchConfiguration("config_file"),
        **pdict(
            "host_ip","sensor_ip","data_port","gnss_port","return_mode",
            "min_range","max_range","frame_id","scan_phase","cloud_min_angle","cloud_max_angle",
            "dual_return_distance_threshold","rotation_speed","packet_mtu_size","setup_sensor","udp_only"
        ),
    }

    nodes.append(
        ComposableNode(
            package="nebula_ros",
            plugin=sensor_make + "RosWrapper",
            name=sensor_make.lower() + "_ros_wrapper_node",
            parameters=[nebula_params],
            # IMPORTANT: Hesai -> Autoware canonical
            remappings=[("pandar_points", "pointcloud_raw_ex")],  # was velodyne_points in stock
            extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
        )
    )

    # CropBox (self vehicle body)
    crop_self_params = {
        "input_frame": LaunchConfiguration("input_frame"),
        "output_frame": LaunchConfiguration("output_frame"),
        "negative": True,
        "min_x": -1000.0, "max_x": 1000.0,
        "min_y": -1000.0, "max_y": 1000.0,
        "min_z": -1000.0, "max_z": 1000.0,
    }
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="crop_box_filter_self",
            remappings=[("input","pointcloud_raw_ex"),("output","self_cropped/pointcloud_ex")],
            parameters=[crop_self_params],
            extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
        )
    )

    # CropBox (mirror region), falls back to passthrough bounds if no mirror params are set
    crop_mirror_params = {
        "input_frame": LaunchConfiguration("input_frame"),
        "output_frame": LaunchConfiguration("output_frame"),
        "negative": True,
        "min_x": mirror_params.get("min_longitudinal_offset", -1000.0),
        "max_x": mirror_params.get("max_longitudinal_offset", 1000.0),
        "min_y": mirror_params.get("min_lateral_offset", -1000.0),
        "max_y": mirror_params.get("max_lateral_offset", 1000.0),
        "min_z": mirror_params.get("min_height_offset", -100.0),
        "max_z": mirror_params.get("max_height_offset", 100.0),
    }
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::CropBoxFilterComponent",
            name="crop_box_filter_mirror",
            remappings=[("input","self_cropped/pointcloud_ex"),("output","mirror_cropped/pointcloud_ex")],
            parameters=[crop_mirror_params],
            extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
        )
    )

    # Distortion corrector (needs /sensing/vehicle_velocity_converter/twist_with_covariance and IMU)
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::DistortionCorrectorComponent",
            name="distortion_corrector_node",
            remappings=[
                ("~/input/twist", "/sensing/vehicle_velocity_converter/twist_with_covariance"),
                ("~/input/imu", "/sensing/imu/imu_data"),
                ("~/input/pointcloud", "mirror_cropped/pointcloud_ex"),
                ("~/output/pointcloud", "rectified/pointcloud_ex"),
            ],
            parameters=[distortion_param],
            extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
        )
    )

    # Ring outlier (finalizes per-lidar chain as pointcloud_before_sync)
    nodes.append(
        ComposableNode(
            package="autoware_pointcloud_preprocessor",
            plugin="autoware::pointcloud_preprocessor::RingOutlierFilterComponent",
            name="ring_outlier_filter",
            remappings=[("input","rectified/pointcloud_ex"),("output","pointcloud_before_sync")],
            parameters=[ring_outlier_param, {"output_frame": LaunchConfiguration("frame_id")}],
            extra_arguments=[{"use_intra_process_comms": LaunchConfiguration("use_intra_process")}],
        )
    )

    container = ComposableNodeContainer(
        name=LaunchConfiguration("pointcloud_container_name"),
        namespace="pointcloud_preprocessor",
        package="rclcpp_components",
        executable=LaunchConfiguration("container_executable"),
        composable_node_descriptions=nodes,
        output="both",
    )
    return [container]

def generate_launch_description():
    launch_arguments = []
    def add_arg(name, default=None, desc=None):
        launch_arguments.append(DeclareLaunchArgument(name, default_value=default, description=desc))

    # Args (kept superset compatible with your older file)
    add_arg("sensor_model")
    add_arg("config_file", "", "path to Hesai Nebula YAML (you pass nebula_config_file above)")
    add_arg("launch_driver", "True")
    add_arg("setup_sensor", "True")
    add_arg("sensor_ip", "192.168.1.201")
    add_arg("host_ip", "255.255.255.255")
    add_arg("scan_phase", "0.0")
    add_arg("base_frame", "base_link")
    add_arg("min_range", "0.3"); add_arg("max_range", "300.0")
    add_arg("cloud_min_angle", "0"); add_arg("cloud_max_angle", "360")
    add_arg("data_port", "2368"); add_arg("gnss_port", "2380")
    add_arg("packet_mtu_size", "1500")
    add_arg("rotation_speed", "600")
    add_arg("dual_return_distance_threshold", "0.1")
    add_arg("frame_id", "hesai_lidar")
    add_arg("input_frame", LaunchConfiguration("base_frame"))
    add_arg("output_frame", LaunchConfiguration("base_frame"))
    add_arg("use_multithread", "False")
    add_arg("use_intra_process", "False")
    add_arg("pointcloud_container_name", "pointcloud_container")  # unified name
    add_arg("vehicle_mirror_param_file", "")
    add_arg("distortion_correction_node_param_path",
            PathJoinSubstitution([FindPackageShare('mkz_sensor_kit_launch'),'config','distortion_corrector_node.param.yaml']))
    add_arg("ring_outlier_filter_node_param_path",
            PathJoinSubstitution([FindPackageShare('mkz_sensor_kit_launch'),'config','ring_outlier_filter_node.param.yaml']))
    add_arg("udp_only", "False")

    set_ce = SetLaunchConfiguration("container_executable","component_container", condition=UnlessCondition(LaunchConfiguration("use_multithread")))
    set_cem = SetLaunchConfiguration("container_executable","component_container_mt", condition=IfCondition(LaunchConfiguration("use_multithread")))

    return launch.LaunchDescription(launch_arguments + [set_ce, set_cem, OpaqueFunction(function=launch_setup)])

