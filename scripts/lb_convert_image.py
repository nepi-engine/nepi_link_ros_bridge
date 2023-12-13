#!/usr/bin/env python
#
# NEPI Dual-Use License
# Project: nepi_link_ros_bridge
#
# This license applies to any user of NEPI Engine software
#
# Copyright (C) 2023 Numurus, LLC <https://www.numurus.com>
# see https://github.com/numurus-nepi/nepi_link_ros_bridge
#
# This software is dual-licensed under the terms of either a NEPI software developer license
# or a NEPI software commercial license.
#
# The terms of both the NEPI software developer and commercial licenses
# can be found at: www.numurus.com/licensing-nepi-engine
#
# Redistributions in source code must retain this top-level comment block.
# Plagiarizing this software to sidestep the license obligations is illegal.
#
# Contact Information:
# ====================
# - https://www.numurus.com/licensing-nepi-engine
# - mailto:nepi@numurus.com
#
#

import rospy
import cv2
from cv_bridge import CvBridge

from sensor_msgs.msg import Image

def lb_convert_image(msg, kwargs=None):
    rospy.loginfo('Converting image with size ' + str(msg.height) + 'x' + str(msg.width))

    bridge = CvBridge()
    cv_image = None
    try:
        cv_image = bridge.imgmsg_to_cv2(msg)
    except:
        rospy.logwarn('Unable to convert image to OpenCV format')
        return None, 0.0

    # TODO: Parametric conversion based on specific keyword args.

    output_filename = kwargs['output_file_basename'] + '.jpg'
    if cv2.imwrite(output_filename, cv_image) is True:
        # TODO: Quality assessment?
        return output_filename, 1.0

    return None, 0.0 # Did not write the file
