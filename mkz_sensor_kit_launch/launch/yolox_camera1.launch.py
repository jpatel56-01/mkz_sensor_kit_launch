from launch import LaunchDescription
from launch.substitutions import EnvironmentVariable
from launch_ros.actions import Node


def generate_launch_description():
    home = EnvironmentVariable("HOME")

    return LaunchDescription(
        [
            Node(
                package="autoware_tensorrt_yolox",
                executable="autoware_tensorrt_yolox_node_exe",
                name="tensorrt_yolox_camera1",
                namespace="/sensing",
                output="screen",
                remappings=[
                    ("/sensing/tensorrt_yolox_camera1/in/image",
                     "/sensing/camera/camera1/image_rect_color"),
                    ("/sensing/tensorrt_yolox_camera1/out/objects",
                     "/perception/object_recognition/detection/rois1"),
                ],
                parameters=[{
                    # Required to avoid statically-typed init crashes
                    "gpu_id": 0,
                    "precision": "fp16",
                    "score_threshold": 0.3,
                    "nms_threshold": 0.7,

                    # Required paths
                    "model_path": [home, "/autoware_data/tensorrt_yolox/yolox-sPlus-opt.onnx"],
                    "label_path": [home, "/autoware_data/tensorrt_yolox/label.txt"],

                    # General behavior
                    "build_only": False,
                    "preprocess_on_gpu": True,

                    # TRT / calibration / quantization (initialize even if unused)
                    "calibration_algorithm": "MinMax",
                    "calibration_image_list_path": "",
                    "clip_value": 0.0,
                    "dla_core_id": -1,
                    "profile_per_layer": False,
                    "quantize_first_layer": False,
                    "quantize_last_layer": False,

                    # ROI overlap / mask overlay features (initialize safely off)
                    "is_publish_color_mask": False,
                    "is_roi_overlap_segment": False,
                    "overlap_roi_score_threshold": 0.0,
                    "color_map_path": [home, "/autoware_data/tensorrt_yolox/semseg_color_map.csv"],

                    # Segment-label overlay toggles (initialize all to False)
                    "roi_overlay_segment_label.UNKNOWN": False,
                    "roi_overlay_segment_label.CAR": False,
                    "roi_overlay_segment_label.TRUCK": False,
                    "roi_overlay_segment_label.BUS": False,
                    "roi_overlay_segment_label.MOTORCYCLE": False,
                    "roi_overlay_segment_label.BICYCLE": False,
                    "roi_overlay_segment_label.PEDESTRIAN": False,
                    "roi_overlay_segment_label.ANIMAL": False,
                }]
            ),
        ]
    )
