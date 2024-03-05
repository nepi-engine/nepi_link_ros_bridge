#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#
import rospy

def lb_ros_msg_to_yaml(msg, kwargs=None):
    # TODO: Parameters to control behavior?

    output_filename = kwargs['output_file_basename'] + '.yaml'

    with open(output_filename, 'w') as f:
        f.write(str(msg))

    return output_filename, 1.0
