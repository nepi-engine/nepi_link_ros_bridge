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

def lb_ros_msg_to_yaml(msg, kwargs=None):
    # TODO: Parameters to control behavior?

    output_filename = kwargs['output_file_basename'] + '.yaml'

    with open(output_filename, 'w') as f:
        f.write(str(msg))

    return output_filename, 1.0
