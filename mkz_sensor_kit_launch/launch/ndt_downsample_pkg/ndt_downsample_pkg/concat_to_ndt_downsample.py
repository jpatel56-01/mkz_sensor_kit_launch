#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    QoSReliabilityPolicy,
    QoSHistoryPolicy,
    QoSDurabilityPolicy,
)

from sensor_msgs.msg import PointCloud2
from sensor_msgs_py import point_cloud2 as pc2


def make_sensor_qos(depth: int = 10) -> QoSProfile:
    """Create a SensorData-like QoS profile (BEST_EFFORT, VOLATILE)."""
    return QoSProfile(
        depth=depth,
        history=QoSHistoryPolicy.KEEP_LAST,
        reliability=QoSReliabilityPolicy.BEST_EFFORT,
        durability=QoSDurabilityPolicy.VOLATILE,
    )


# ----------------------------------------------------------------------
# Filter parameters (tune here, no YAML needed) 
# chmod +x ndt_downsample_pkg/concat_to_ndt_downsample.py

# ----------------------------------------------------------------------
# Measurement range [m] (radial distance from sensor)
MIN_RANGE = 1.0
MAX_RANGE = 70.0

# Voxel grid leaf size [m]
VOXEL_LEAF_SIZE = 0.30

# Random downsample ratio (1.0 = keep all)
RANDOM_KEEP_RATIO = 1.0
# ----------------------------------------------------------------------


class ConcatToNdtDownsample(Node):
    """LiDAR preprocessor for NDT: crop-range + voxel + random downsample.

    Subscribes:
        /sensing/lidar/concatenated/pointcloud  (PointXYZIRCAEDT layout)

    Publishes:
        /localization/util/downsample/pointcloud  (same layout)
    """

    def __init__(self) -> None:
        super().__init__("concat_to_ndt_downsample")

        sensor_qos = make_sensor_qos()

        # Input: concatenated lidar cloud
        self.sub = self.create_subscription(
            PointCloud2,
            "/sensing/lidar/concatenated/pointcloud",
            self.on_cloud,
            sensor_qos,
        )

        # Output: filtered cloud for NDT
        self.pub = self.create_publisher(
            PointCloud2,
            "/localization/util/downsample/pointcloud",
            sensor_qos,
        )

        self.count_in = 0
        self.count_out = 0

        self.get_logger().info(
            "concat_to_ndt_downsample running:\n"
            "  input : /sensing/lidar/concatenated/pointcloud\n"
            "  output: /localization/util/downsample/pointcloud\n"
            f"  MIN_RANGE={MIN_RANGE} m, MAX_RANGE={MAX_RANGE} m, "
            f"VOXEL_LEAF_SIZE={VOXEL_LEAF_SIZE} m, "
            f"RANDOM_KEEP_RATIO={RANDOM_KEEP_RATIO}"
        )

    # ------------------------------------------------------------------ #
    # Core callback
    # ------------------------------------------------------------------ #

    def on_cloud(self, msg: PointCloud2) -> None:
        """Receive a PointXYZIRCAEDT cloud, filter, and republish."""

        self.count_in += 1

        # 1) Read all points as tuples, but also store xyz separately
        points = []     # full tuples (x,y,z,intensity,ring,classification,azimuth,distance,time)
        xyz_list = []   # just xyz for filtering

        for p in pc2.read_points(msg, skip_nans=True):
            points.append(p)
            xyz_list.append(p[0:3])

        if not points:
            return

        xyz = np.asarray(xyz_list, dtype=np.float32)
        idx = np.arange(len(points))

        # 2) Measurement range filter (based on radial distance)
        r = np.linalg.norm(xyz, axis=1)
        mask = (r >= MIN_RANGE) & (r <= MAX_RANGE)
        idx = idx[mask]
        xyz = xyz[mask]
        if idx.size == 0:
            return

        # 3) Voxel grid downsample
        voxel_indices = np.floor(xyz / VOXEL_LEAF_SIZE).astype(np.int32)
        _, unique_idx = np.unique(voxel_indices, axis=0, return_index=True)
        idx = idx[unique_idx]
        xyz = xyz[unique_idx]
        if idx.size == 0:
            return

        # 4) Optional random downsample
        if RANDOM_KEEP_RATIO < 1.0:
            keep_n = max(1, int(idx.size * RANDOM_KEEP_RATIO))
            choice = np.random.choice(idx.size, keep_n, replace=False)
            idx = idx[choice]
            # xyz = xyz[choice]     # only needed for debugging / logging

        # 5) Build filtered point list with full field layout
        filtered_points = [points[i] for i in idx]

        # 6) Recreate PointCloud2 with ORIGINAL fields (PointXYZIRCAEDT)
        filtered_msg = pc2.create_cloud(
            msg.header,
            msg.fields,      # keep layout exactly as input
            filtered_points,
        )

        self.pub.publish(filtered_msg)
        self.count_out += 1

        # Occasional debug
        if self.count_in % 50 == 0:
            self.get_logger().info(
                f"[concat_to_ndt_downsample] in_clouds={self.count_in}, "
                f"out_clouds={self.count_out}, "
                f"last_out_points={len(filtered_points)}"
            )


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ConcatToNdtDownsample()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()

