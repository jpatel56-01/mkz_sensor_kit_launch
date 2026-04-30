#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    QoSProfile,
    QoSReliabilityPolicy,
    QoSHistoryPolicy,
    QoSDurabilityPolicy,
)
from sensor_msgs.msg import PointCloud2


def make_sensor_qos(depth: int = 10) -> QoSProfile:
    return QoSProfile(
        depth=depth,
        history=QoSHistoryPolicy.KEEP_LAST,
        reliability=QoSReliabilityPolicy.BEST_EFFORT,
        durability=QoSDurabilityPolicy.VOLATILE,
    )


class ConcatToNdtDownsample(Node):
    def __init__(self):
        super().__init__('concat_to_ndt_downsample')

        sensor_qos = make_sensor_qos()

        # Sub from LiDAR (BEST_EFFORT QoS on concatenated cloud)
        self.sub = self.create_subscription(
            PointCloud2,
            '/sensing/lidar/concatenated/pointcloud',
            self.on_cloud,
            sensor_qos,
        )

        # Pub to NDT input topic (same QoS)
        self.pub = self.create_publisher(
            PointCloud2,
            '/localization/util/downsample/pointcloud',
            sensor_qos,
        )

        self.count = 0
        self.get_logger().info(
            'Forwarding /sensing/lidar/concatenated/pointcloud '
            '→ /localization/util/downsample/pointcloud '
            'with BEST_EFFORT sensor QoS'
        )

    def on_cloud(self, msg: PointCloud2):
        self.pub.publish(msg)
        self.count += 1
        if self.count % 50 == 0:
            self.get_logger().info(f'Forwarded {self.count} clouds')


def main(args=None):
    rclpy.init(args=args)
    node = ConcatToNdtDownsample()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()


