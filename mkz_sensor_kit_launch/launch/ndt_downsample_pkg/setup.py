from setuptools import find_packages, setup

package_name = 'ndt_downsample_pkg'

setup(
    name=package_name,
    version='0.0.1',
    packages=find_packages(exclude=['test']),
    data_files=[
        # Install the ament resource index file so `ros2` can find the package
        ('share/ament_index/resource_index/packages',
         ['resource/' + package_name]),
        # Install package.xml
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='mkz3',
    maintainer_email='jpatel56@vols.utk.edu',
    description=(
        'Forward /sensing/lidar/concatenated/pointcloud '
        'to /localization/util/downsample/pointcloud with sensor QoS.'
    ),
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            # ros2 run ndt_downsample_pkg concat_to_ndt_downsample
            # → calls main() in ndt_downsample_pkg/concat_to_ndt_downsample.py
            'concat_to_ndt_downsample = '
            'ndt_downsample_pkg.concat_to_ndt_downsample:main',
        ],
    },
)

