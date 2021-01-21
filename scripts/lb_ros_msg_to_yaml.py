import rospy

def lb_ros_msg_to_yaml(msg, kwargs=None):
    # TODO: Parameters to control behavior?

    output_filename = kwargs['output_file_basename'] + '.yaml'

    with open(output_filename, 'w') as f:
        f.write(str(msg))

    return output_filename, 1.0
