#!/usr/bin/env python
#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
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
