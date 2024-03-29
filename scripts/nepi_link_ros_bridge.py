#!/usr/bin/env python
#
# Copyright (c) 2024 Numurus, LLC <https://www.numurus.com>.
#
# This file is part of nepi-engine
# (see https://github.com/nepi-engine).
#
# License: 3-clause BSD, see https://opensource.org/licenses/BSD-3-Clause
#
import threading
from importlib import import_module
from datetime import datetime
import os
from shutil import copyfile
import json
from math import degrees
import glob

import rospy
import rosservice
from tf.transformations import euler_from_quaternion

from std_msgs.msg import Bool, Empty, Float32
from nepi_ros_interfaces.msg import StringEnable
from nepi_ros_interfaces.srv import NEPILinkStatusQuery, NEPILinkStatusQueryResponse
from nepi_ros_interfaces.msg import SaveData, SaveDataRate

# Following is used for 3DX-specific NEPI LB Status creation -- remove when that
# functionality is made generic
from nepi_ros_interfaces.srv import NavPoseQuery, NavPoseQueryRequest, NavPoseQueryResponse
from nepi_ros_interfaces.msg import SystemStatus

from nepi_edge_sdk_base.save_cfg_if import SaveCfgIF

from nepi_edge_sdk_link import *

# Default params
DEFAULT_ENABLED = False                                             
DEFAULT_AUTO_ATTEMPTS_PER_HOUR = 6                                  
DEFAULT_NEPI_BOT_ROOT_FOLDER = '/opt/nepi/nepi_link/nepi-bot'       
DEFAULT_NEPI_LOG_STORAGE_ENABLED = False                            
DEFAULT_NEPI_LOG_STORAGE_FOLDER = '/mnt/nepi-storage/data/nepi-bot'   
DEFAULT_REBOOT_SYS_ON_SW_UPDATE = True                              

DEFAULT_LB_ENABLED = True
DEFAULT_LB_SESSION_MAX_TIME = 60  
DEFAULT_LB_DATA_SETS_PER_HOUR = 12    
DEFAULT_LB_MAX_DATA_WAIT_TIME_S = 30.0

DEFAULT_HB_ENABLED = True                             
DEFAULT_HB_SESSION_MAX_TIME_S = 300                   
DEFAULT_HB_DATA_SOURCE_FOLDER = '/mnt/nepi-storage/data'
DEFAULT_HB_AUTO_DATA_OFFLOAD = False                  

DEFAULT_SNAPSHOT_TRIGGERS_LB = True
DEFAULT_SNAPSHOT_SAVE_RATE_HZ = 1.0                  
DEFAULT_SNAPSHOT_ONBOARD_SAVE_DURATION_S = 2.0              

def getDirectorySize(start_path):
    total_size = 0
    for dpath, dnames, fnames in os.walk(start_path):
        for f in fnames:
            fp = os.path.join(dpath, f)
            # Don't skip symbolic links -- follow them
            total_size += os.path.getsize(fp)
    return total_size

class NEPILinkRosBridge:
    NODE_NAME = "nepi_link_ros_bridge"

    def autoConnectTimerExpired(self, timer):
        rospy.loginfo('Scheduled auto-connect session starting')

        # Start BOT and follow-up processing
        self.runNEPIBot()
        # TODO: Anything else?

    def runNEPIBot(self):
        # Query param server for LB and HB params and launch nepi-bot
        # Ensure this only runs once at a time -- this lock is held through the entire execution of nepi-bot
        #if self.nepi_bot_lock.acquire(blocking=False) is True: # Python 3
        if self.nepi_bot_lock.acquire(False) is True:
            self.bot_running = True

            # First, setup the HB data offload if so configured
            do_data_offload = rospy.get_param('~hb/auto_data_offload', DEFAULT_HB_AUTO_DATA_OFFLOAD)
            if do_data_offload is True:
                data_folder = rospy.get_param('~hb/data_source_folder', DEFAULT_HB_DATA_SOURCE_FOLDER)
                self.nepi_sdk.linkHBDataFolder(data_folder)
            else:
                self.nepi_sdk.unlinkHBDataFolder() # No harm in doing this unnecessarily

            # Get the current timestamp for use as a directory name later
            start_time_subdir_name = datetime.utcnow().replace(microsecond=0).isoformat().replace(':', '')

            # If LB is enabled, we are required (by NEPI Policy) to create a brand-new status message
            lb_enabled = rospy.get_param('~lb/enabled', DEFAULT_LB_ENABLED)
            if lb_enabled is True:
                rospy.loginfo('Gathering an updated LB status for the upcoming LB session')
                max_status_wait_time_s = rospy.get_param('~lb/max_data_wait_time_s', DEFAULT_LB_MAX_DATA_WAIT_TIME_S)
                self.createNEPILinkStatus(timeout_s=max_status_wait_time_s, export_on_complete=True)

            # Run the bot and wait for it to complete
            lb_timeout_s = rospy.get_param('~lb/session_max_time_s', DEFAULT_LB_SESSION_MAX_TIME)
            hb_enabled = rospy.get_param('~hb/enabled', DEFAULT_HB_ENABLED)
            hb_timeout_s = rospy.get_param('~hb/session_max_time_s', DEFAULT_HB_SESSION_MAX_TIME_S)
            rospy.loginfo('Starting NEPI-BOT at ' + start_time_subdir_name +
                          '\n\tRun LB = ' + str(lb_enabled) + ' (' + str(lb_timeout_s) + 's timeout)' +
                          '\n\tRun HB = ' + str(lb_enabled) + ' (' + str(hb_timeout_s) + 's timeout)')
            self.nepi_sdk.startBot(lb_enabled, lb_timeout_s, hb_enabled, hb_timeout_s)

            # Set up some timing control
            bot_start_time = rospy.get_rostime()
            bot_max_duration = rospy.Duration.from_sec(max(lb_timeout_s, hb_timeout_s) + 1.0)
            total_failure_duration = bot_max_duration + rospy.Duration.from_sec(20.0)
            hard_kill_duration = bot_max_duration + rospy.Duration.from_sec(10.0)

            # Now wait for completion -- monitor timeout in case something fails in SDK
            rospy.sleep(0.5) # To let nepi-bot start up completely
            while (self.nepi_sdk.checkBotRunning() is True):
                bot_duration = rospy.get_rostime() - bot_start_time
                if (bot_duration > total_failure_duration):
                    rospy.logerr_throttle(10, 'Cannot kill NEPI-Bot process -- unable to continue')
                elif (bot_duration > hard_kill_duration):
                    rospy.logerr('Attempting to kill NEPI-Bot because max duration was greatly exceeded')
                    self.nepi_sdk.stopBot(force_kill=True)
                elif (bot_duration > bot_max_duration):
                    rospy.logwarn('Signaling NEPI-Bot to shut down because max duration was exceeded')
                    self.nepi_sdk.stopBot(force_kill=False)

                rospy.sleep(2.0)

            bot_run_duration = rospy.get_rostime() - bot_start_time
            rospy.loginfo('NEPI-Bot Execution Complete (' + str(bot_run_duration.to_sec()) + ' secs)')

            # At completion, import the status file into class members -- do this while continuing to hold the lock
            # as nepi-bot would delete the status files if it happened to run again
            exec_status = NEPIEdgeExecStatus()
            (lb_statuses, hb_statuses, software_updated) = exec_status.importStatus()

            # Update fields for any new connections
            for lb_stat in lb_statuses:
                # This logic will pick up the _last_ successful LB comms link in the list.
                # There should be at most one of these, but that assumption isn't checked here
                if lb_stat['comms_status'] == NEPI_EDGE_COMMS_STATUS_SUCCESS:
                    # Timestamp conversion
                    start_time_rfc3339 = lb_stat['start_time_rfc3339']
                    #start_time_unix = datetime.strptime(start_time_rfc3339, '%Y-%m-%dT%H:%M:%S.%f').timestamp() # Python 3
                    start_time_unix = (datetime.strptime(start_time_rfc3339, '%Y-%m-%dT%H:%M:%S.%f') - datetime(1970, 1, 1)).total_seconds()
                    self.lb_last_connection_time = rospy.Time(start_time_unix)
                    self.lb_do_msg_count = lb_stat['msgs_sent']
                    self.lb_dt_msg_count = lb_stat['msgs_rcvd']

            for hb_stat in hb_statuses:
                # There could be multiple successful HB sessions in here and we report
                # one timestamp, so we need a policy -- it is 'report the last success timestamp
                # in the list'
                if hb_stat['comms_status'] == NEPI_EDGE_COMMS_STATUS_SUCCESS:
                    start_time_rfc3339 = hb_stat['start_time_rfc3339']
                    #start_time_unix = datetime.strptime(start_time_rfc3339, '%Y-%m-%dT%H:%M:%S.%f').timestamp() # Python 3
                    start_time_unix = (datetime.strptime(start_time_rfc3339, '%Y-%m-%dT%H:%M:%S.%f') - datetime(1970, 1, 1)).total_seconds()
                    self.hb_last_connection_time = rospy.Time(start_time_unix)

                    if hb_stat['direction'] == NEPI_EDGE_HB_DIRECTION_DO:
                        self.hb_do_transfered_mb = float(hb_stat['datasent_kB']) / 1000.0
                    elif hb_stat['direction'] == NEPI_EDGE_HB_DIRECTION_DT:
                        self.hb_dt_transfered_mb = float(hb_stat['datareceived_kB']) / 1000.0

            # Copy all the log files if so configured -- do this while continuing to hold the lock
            # as nepi-bot might delete the log files if it happened to run again
            if rospy.get_param('~nepi_log_storage_enabled', DEFAULT_NEPI_LOG_STORAGE_ENABLED) is True:
                nepi_log_storage_folder = os.path.join(rospy.get_param('~nepi_log_storage_folder', DEFAULT_NEPI_LOG_STORAGE_FOLDER), start_time_subdir_name)
                if not os.path.exists(nepi_log_storage_folder):
                    os.makedirs(nepi_log_storage_folder)

                log_src_folder = os.path.join(rospy.get_param('~nepi_bot_root_folder', DEFAULT_NEPI_BOT_ROOT_FOLDER), 'log')
                src_files = os.listdir(log_src_folder)
                for f in src_files:
                    full_file_name = os.path.join(log_src_folder, f)
                    if (os.path.isfile(full_file_name)):
                        # Copy the log file and then delete it from its source -- this is the default
                        # behavior under nepi_log_storage_enabled
                        copyfile(full_file_name, os.path.join(nepi_log_storage_folder,f))
                        os.remove(full_file_name)

            self.bot_running = False

            # If configured, reboot the system now
            if (software_updated is True) and (rospy.get_param('~reboot_sys_on_sw_update', DEFAULT_REBOOT_SYS_ON_SW_UPDATE) is True):
                rospy.logwarn("Rebooting now because s/w was updated via NEPI")
                rospy.sleep(1.0)
                os.system('reboot now')
            else:
                rospy.loginfo("Not rebooting because software_updated is " + str(software_updated) + ' and reboot_sys_on_sw_update is ' + str(rospy.get_param('~reboot_sys_on_sw_update', DEFAULT_REBOOT_SYS_ON_SW_UPDATE)))

            # Must ALWAYS release the lock
            self.nepi_bot_lock.release()

        else: # Unable to acquire
            rospy.logwarn("Unable to run nepi-bot because it is already running in another thread")

    def getAndConvertDataMessage(self, timeout,
                                 topic, msg_type, msg_mod,
                                 snippet_type, snippet_id,
                                 conversion_call,
                                 is_service_response,
                                 service_req_kwargs=None,
                                 conversion_parameters=None,):
        # Append the root namespace if necessary, i.e., if the supplied topic name is a relative namespace
        fully_qualified_topic = topic
        if topic[0] != '/': # Relative name
            fully_qualified_topic = rospy.get_namespace() + topic

        try:
            mod = import_module(msg_mod)
            msg_type_object = getattr(mod, msg_type)
            if not is_service_response:
                msg = rospy.wait_for_message(fully_qualified_topic, msg_type_object, timeout)
            else:
                # Calling services is a bit more involved
                # Create the service and service request objects
                service_object = getattr(mod, msg_type)
                service_req_object = getattr(mod, msg_type + 'Request')

                # Construct the service request -- provide keyword args if any were supplied
                service_request = None
                if service_req_kwargs is not None:
                    service_request = service_req_object(service_req_kwargs)
                else:
                    service_request = service_req_object()

                # Create the proxy and call it with the request
                service_query = rospy.ServiceProxy(fully_qualified_topic, service_object)
                msg = service_query(service_request)

        except rospy.ROSException:
            rospy.logwarn('Timeout while waiting for ' + topic + ' for LB data')
            return
        except AttributeError:
            rospy.logwarn('Message type ' + msg_type + ' not in package ' + msg_mod)
            return
        except ImportError:
            rospy.logwarn('No module ' + msg_mod + '.msg')
            return

        # Now import and call the conversion routine
        try:
            # TODO: Do we need a more flexible approach than requiring each conversion call
            # comes as a function in a module (.py file) by that same name?
            mod = import_module(conversion_call)
            conversion_function = getattr(mod, conversion_call)
        except ImportError:
            rospy.logwarn('Conversion module ' + conversion_call + ' missing or invalid')
            return
        except AttributeError:
            rospy.logwarn('No conversion function ' + conversion_call + ' in module ' + conversion_call)
            return

        output_file_base = topic.replace('/','.').strip('.')
        conversion_args = {'output_file_basename': output_file_base}
        if conversion_parameters is not None:
            conversion_args.update(conversion_parameters)
        data_filename, conversion_quality = conversion_function(msg, kwargs=conversion_args)

        # TODO: Gather updated nav/pose info for the data snippet here

        # Next create the data snippet object
        snippet = NEPIEdgeLBDataSnippet(snippet_type, snippet_id)
        # And set the file
        if data_filename is not None:
            snippet.setOptionalFields(data_file = data_filename, delete_data_file_after_export = True)

        # Finally, under lock protection add this to the list of data snippets
        with self.nepi_data_snippets_lock:
            self.latest_nepi_data_snippets.append(snippet)

    def createNEPILinkStatus(self, timeout_s, export_on_complete = False):
        start_time_ros = rospy.get_rostime()
        timeout_ros_remaining = rospy.Duration.from_sec(timeout_s)

        # Populate some of the optional fields. For now this will be hardcoded to the nav_pose query response, but eventually will be based on
        # a parameter mapping defined in the config file
        nav_pose_req = NavPoseQueryRequest() # Default constructor will set the query_time = {0,0} as we want
        nav_pose_req.transform = True
        nav_pose_query = rospy.ServiceProxy('nav_pose_query', NavPoseQuery)
        try:
            nav_pose_resp = nav_pose_query(nav_pose_req)
        except:
            rospy.logwarn("Failed to obtain nav/pos info for NEPI Link status message... nulling these fields")
            nav_pose_resp = NavPoseQueryResponse()

        # Compute some special formats from the response
        navsat_fix_time_posix = nav_pose_resp.nav_pose.fix.header.stamp.secs + (nav_pose_resp.nav_pose.fix.header.stamp.nsecs / 1000000000.0)
        fix_time_rfc3339 = datetime.fromtimestamp(navsat_fix_time_posix).isoformat()
        heading_ref_from_bool = NEPI_EDGE_HEADING_REF_MAG_NORTH # This is no longer specified by nav_pose_mgr, so here it is just hard-coded
        quaternion_orientation = (nav_pose_resp.nav_pose.odom.pose.pose.orientation.x, nav_pose_resp.nav_pose.odom.pose.pose.orientation.y,
                                  nav_pose_resp.nav_pose.odom.pose.pose.orientation.z, nav_pose_resp.nav_pose.odom.pose.pose.orientation.w)
        euler_orientation_rad = euler_from_quaternion(quaternion_orientation)

        # Update the timeout period
        elapsed_ros = rospy.get_rostime() - start_time_ros
        timeout_ros_remaining -= elapsed_ros
        if (timeout_ros_remaining.to_sec() < 0.0):
            rospy.logwarn("Timeout during nav_pose_status_query")
            return

        # Temperature comes from the status message, which we must wait for
        status_topic = rospy.get_namespace() + 'system_status'
        try:
            sys_status_msg = rospy.wait_for_message(status_topic, SystemStatus, timeout_ros_remaining.to_sec())
        except rospy.ROSException:
            rospy.logwarn("Timeout while waiting for system_status")
            return

        with self.latest_nepi_status_lock:
            self.latest_nepi_status = None # Clear it
            # Ensure we always have a bare-minimum nepi-status after this call
            self.latest_nepi_status = NEPIEdgeLBStatus(datetime.utcnow().isoformat())
            self.latest_nepi_status.setOptionalFields(navsat_fix_time_rfc3339 = fix_time_rfc3339,
                                                      latitude_deg = nav_pose_resp.nav_pose.fix.latitude, longitude_deg = nav_pose_resp.nav_pose.fix.longitude,
                                                      heading_ref = heading_ref_from_bool, heading_deg = nav_pose_resp.nav_pose.heading.heading,
                                                      roll_angle_deg = degrees(euler_orientation_rad[0]), pitch_angle_deg = degrees(euler_orientation_rad[1]))
            self.latest_nepi_status.setOptionalFields(temperature_c = sys_status_msg.temperatures[0])
            if export_on_complete is True:
                empty_snippets = []
                self.latest_nepi_status.export(empty_snippets)

    def lbDataCollectionTimerExpired(self, timer):
        rospy.loginfo('Scheduled LB data collection session starting')
        self.createLBDataSet()

    def eventTriggeredSaveDataTimerExpired(self, timer):
        stop_save_data_msg = SaveData()
        stop_save_data_msg.save_continuous = False
        stop_save_data_msg.save_raw = False
        self.save_data_pub.publish(stop_save_data_msg)
        self.onboard_save_timer.shutdown() # One-shot timer

    def createLBDataSet(self):
        rospy.loginfo('Creating LB Data set ')
        # First, launch the status message thread
        wait_for_message_threads = {}
        max_data_wait_time_s = rospy.get_param('~lb/max_data_wait_time_s', DEFAULT_LB_MAX_DATA_WAIT_TIME_S)

        # New data set, so clear snippets
        with self.nepi_data_snippets_lock:
            self.latest_nepi_data_snippets[:] = [] # Python 2.7 compatible

        # Start a status query in a separate thread
        wait_for_message_threads['nepi_status'] = threading.Thread(target=self.createNEPILinkStatus, kwargs={'timeout_s': max_data_wait_time_s})
        wait_for_message_threads['nepi_status'].start()

        supported_msg_types = self.getHandledMsgTypes()
        enabled_topics = rospy.get_param("~lb/enabled_topics")
        available_topics = rospy.get_published_topics()

        running_snippet_ids = {}
        base_namespace = rospy.get_namespace()
        for entry in available_topics:
            msg_type = entry[1]
            topic_name = entry[0]
            topic_name_short = topic_name[len(base_namespace):] if topic_name.startswith(base_namespace) else topic_name

            # Filter out any topics that have unsupported message types or are not currently enabled for LB
            if (msg_type not in supported_msg_types) or \
               ((topic_name not in enabled_topics) and (topic_name_short not in enabled_topics)):
                continue

            # Remove entry from enabled_topics for better performance on next loop and also so that we can
            # report on any topics that are "enabled" but not advertised yet.
            try:
                enabled_topics.remove(topic_name)
            except:
                enabled_topics.remove(topic_name_short)

            # Gather the dictionary of per-type settings
            msg_type_settings = supported_msg_types[msg_type]

            # Ensure snippet IDs are unique for each topic with a particular message type
            if msg_type in running_snippet_ids:
                running_snippet_ids[msg_type] += 1
            else:
                running_snippet_ids[msg_type] = 0

            # Generate the python name for the module that includes this message type and name of the object
            python_msg_module, python_msg_type = msg_type.split('/')
            python_msg_module += '.msg'

            wait_thread_args = {'timeout': max_data_wait_time_s,
                                'topic': topic_name, 
                                'snippet_type': msg_type_settings['snippet_type'],
                                'snippet_id': running_snippet_ids[msg_type],
                                'conversion_call': msg_type_settings['conversion_call'],
                                'msg_mod': python_msg_module,
                                'msg_type': python_msg_type,
                                'is_service_response': False}
            wait_for_message_threads[topic_name] = threading.Thread(target=self.getAndConvertDataMessage, kwargs=wait_thread_args)
            wait_for_message_threads[topic_name].start()
        
        for entry in enabled_topics:
            rospy.loginfo("LB enabled topic " + entry + " is not currently provided by any node.")

        # Now do the same for services that are enabled
        available_services = rosservice.get_service_list()
        enabled_services = rospy.get_param('~lb/enabled_services')
        supported_srv_types = self.getHandledSrvTypes()
        for service in enabled_services:
            if not service.startswith('/'):
                service = rospy.get_namespace() + service # Fully qualified
            # Filter out any services that are not currently being provided
            if service not in available_services:
                rospy.logwarn("LB enabled service " + service + " is not currently provided by any node")
                continue

            # Filter out any services whose service type is not handled
            service_type = rosservice.get_service_type(service)
            if service_type not in supported_srv_types:
                rospy.logwarn("Service " + service + " of type " + service_type + " is of unconfigured/unsupported type")
                continue

            if service_type in running_snippet_ids:
                running_snippet_ids[service_type] += 1
            else:
                running_snippet_ids[service_type] = 0

            python_srv_module, python_srv_type = service_type.split('/')
            python_srv_module += '.srv'
            
            service_type_settings = supported_srv_types[service_type]
            wait_thread_args = {'timeout': max_data_wait_time_s,
                                'topic': service,
                                'snippet_type': service_type_settings['snippet_type'],
                                'snippet_id': running_snippet_ids[service_type],
                                'conversion_call': service_type_settings['conversion_call'],
                                'msg_type': python_srv_type,
                                'msg_mod': python_srv_module,
                                'is_service_response': True}
            if 'request_args' in service_type_settings:
                wait_thread_args['service_req_kwargs'] = service_type_settings['request_args']

            wait_for_message_threads[service] = threading.Thread(target=self.getAndConvertDataMessage, kwargs=wait_thread_args)
            wait_for_message_threads[service].start()

        # Now wait for all the child threads to terminate
        for t in wait_for_message_threads:
            wait_for_message_threads[t].join()

        # And export the status+data
        if self.latest_nepi_status is not None:
            self.latest_nepi_status.export(self.latest_nepi_data_snippets)
        else:
            rospy.logwarn('NEPI LB Status message was not properly constructed')

    def updateAutoConnectionScheduler(self, enabled, rate_per_hour):
        if (enabled is True) and (rate_per_hour > 0.0):
            rospy.loginfo("Enabling auto connect scheduler (" + str(rate_per_hour) + "x/hr)")
            duration_s = 3600.0 / rate_per_hour
            if self.auto_connect_timer is not None:
                self.auto_connect_timer.shutdown()
            self.auto_connect_timer = rospy.Timer(rospy.Duration(duration_s), self.autoConnectTimerExpired)
        elif self.auto_connect_timer is not None:
            rospy.logwarn('Disabling auto connect scheduler')
            self.auto_connect_timer.shutdown()
            self.auto_connect_timer = None
        # Otherwise, the time is already shut down

    def updateDataCollectionScheduler(self, enabled, rate_per_hour):
        if (enabled is True) and (rate_per_hour > 0.0):
            rospy.loginfo("Enabling LB data collection scheduler (" + str(rate_per_hour) + "x/hr)")
            duration_s = 3600.0 / rate_per_hour
            if self.lb_data_collection_timer is not None:
                self.lb_data_collection_timer.shutdown()
            self.lb_data_collection_timer = rospy.Timer(rospy.Duration(duration_s), self.lbDataCollectionTimerExpired)
        elif self.lb_data_collection_timer is not None:
            rospy.logwarn('Disabling LB data collection scheduler')
            self.lb_data_collection_timer.shutdown()
            self.lb_data_collection_timer = None
        # Otherwise, the time is already shut down

    def extractFieldsFromNEPIBotConfig(self):
        # Query param server for nepi-bot root folder and use that to determine nepi-bot config file
        # Open config file (read-only) and find the fields of interest
        nepi_bot_root_folder = rospy.get_param('~nepi_bot_root_folder', DEFAULT_NEPI_BOT_ROOT_FOLDER)
        nepi_bot_cfg_file = os.path.join(nepi_bot_root_folder, 'cfg/bot/config.json')
        with open(nepi_bot_cfg_file, 'r') as f:
            cfg_dict = json.load(f)
        return cfg_dict['alias'], cfg_dict['lb_conn_order']
    
    def extractPublicSSHKeyString(self):
        # Query param server for nepi-bot root folder and use that to determine nepi-bot SSH key
        nepi_bot_root_folder = rospy.get_param('~nepi_bot_root_folder', DEFAULT_NEPI_BOT_ROOT_FOLDER)
        nepi_bot_ssh_pub_key_wildcard_search = os.path.join(nepi_bot_root_folder, 'devinfo/id*pub')
        nepi_bot_ssh_pub_key_files = glob.glob(nepi_bot_ssh_pub_key_wildcard_search)
        keyfile_count = len(nepi_bot_ssh_pub_key_files)
        if keyfile_count == 0:
            rospy.logerr("No nepi-bot public SSH keyfile detected.")
            return "file not found"
        
        if keyfile_count > 1:
            rospy.logwarn("Found " + str(keyfile_count) + " nepi-bot public SSH keyfiles... will report the first one")

        with open(nepi_bot_ssh_pub_key_files[0], 'r') as f:
            return f.read()

    def updateFromParamServer(self):
        # Most parameters are handled on an as-needed basis, so here we only
        # deal with those that control top-level behavior

        enabled = rospy.get_param("~enabled", DEFAULT_ENABLED)
        auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", DEFAULT_AUTO_ATTEMPTS_PER_HOUR)
        self.updateAutoConnectionScheduler(enabled, auto_attempts_per_hour)

        # Only apply the data_sets_per_hour if top-level enabled is true
        lb_enabled = rospy.get_param("~lb/enabled", DEFAULT_LB_ENABLED)
        lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", DEFAULT_LB_DATA_SETS_PER_HOUR)
        run_data_collection = enabled and lb_enabled
        self.updateDataCollectionScheduler(run_data_collection, lb_data_sets_per_hour)

    def enableNEPIEdge(self, msg):
        # Check if this is truly an update -- if not, don't do anything
        prev_enabled = rospy.get_param('~enabled', DEFAULT_ENABLED)
        new_enabled = msg.data
        if prev_enabled != new_enabled:
            rospy.logwarn("Setting NEPI Edge enabled to " + str(new_enabled))
            # Update the param server
            rospy.set_param("~enabled", new_enabled)

            # Now just reinitialize everything as if we were just starting up with this value
            self.updateFromParamServer()

    def connectNow(self, msg):
        enabled = rospy.get_param("~enabled", DEFAULT_ENABLED)
        if enabled is True:
            # Simply a pass-through to runNEPIBot()
            rospy.loginfo("Connecting now by command")
            self.runNEPIBot()
        else:
            rospy.logwarn("Cannot connect now -- NEPI Edge is currently disabled")

    def createLBDataSetNow(self, msg):
        self.createLBDataSet()

    def setAutoAttemptsPerHour(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_auto_attempts_per_hour = rospy.get_param('~auto_attempts_per_hour', DEFAULT_AUTO_ATTEMPTS_PER_HOUR)
        new_auto_attempts_per_hour = msg.data
        if prev_auto_attempts_per_hour != new_auto_attempts_per_hour:
            # Update the param server
            rospy.set_param('~auto_attempts_per_hour', new_auto_attempts_per_hour)
            # Update the scheduler
            enabled = rospy.get_param('~enabled', DEFAULT_ENABLED)
            self.updateAutoConnectionScheduler(enabled, new_auto_attempts_per_hour)

    def enableNEPIBotLogStorage(self, msg):
        # Nothing to do here but update the param server
        rospy.loginfo("Log Storage: " + ("Enabled" if (msg.data == True) else "Disabled"))
        rospy.set_param('~nepi_log_storage_enabled', msg.data)

    def enableLB(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_lb_enabled = rospy.get_param('~lb/enabled', DEFAULT_LB_ENABLED)
        new_lb_enabled = msg.data
        if prev_lb_enabled != new_lb_enabled:
            rospy.loginfo("LB: " + ("Enabled" if (msg.data == True) else "Disabled"))
            # Update the param server
            rospy.set_param('~lb/enabled', new_lb_enabled)
            # Update the scheduler
            lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", DEFAULT_LB_DATA_SETS_PER_HOUR)
            self.updateDataCollectionScheduler(new_lb_enabled, lb_data_sets_per_hour)

    def setLBDataSetsPerHour(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_sets_per_hour = rospy.get_param('~lb/data_sets_per_hour', DEFAULT_LB_DATA_SETS_PER_HOUR)
        new_sets_per_hour = msg.data
        if prev_sets_per_hour != new_sets_per_hour:
            # Update the param server
            rospy.set_param('~lb/data_sets_per_hour', new_sets_per_hour)
            # Update the scheduler
            lb_enabled = rospy.get_param('~lb/enabled', DEFAULT_LB_ENABLED)
            self.updateDataCollectionScheduler(lb_enabled, new_sets_per_hour)

    def selectLBDataSource(self, msg):
        enabled_topics = rospy.get_param('~lb/enabled_topics', [])
        enabled_services = rospy.get_param('~lb/enabled_services', [])

        ros_basename = rospy.get_namespace()
        source_long_name = msg.entry if msg.entry.startswith(ros_basename) else (ros_basename + msg.entry)
        source_short_name = source_long_name[len(ros_basename):]
        
        if msg.enable is False: # Trying to disable a data source
            rospy.loginfo("Disabling data source %s by request", msg.entry)
            if source_long_name in enabled_topics:
                enabled_topics.remove(source_long_name)
            elif source_short_name in enabled_topics:
                enabled_topics.remove(source_short_name)
            elif source_long_name in enabled_services:
                enabled_services.remove(source_long_name)
            elif source_short_name in enabled_services:
                enabled_services.remove(source_short_name)
            else:
                rospy.logwarn("%s is not currently enabled as LB data source... ignoring disable request", msg.entry)
                return
        else: # Trying to enable a data source
            rospy.loginfo("Enabling topic data source %s by request", msg.entry)

            # Must determine if this is topic or service
            available_topics = rospy.get_published_topics()
            available_services = rosservice.get_service_list()
        
            is_service = False
            is_topic = any(source_long_name in sublist for sublist in available_topics) or \
                       any(source_short_name in sublist for sublist in available_topics)
            if not is_topic:
                is_service = (source_long_name in available_services) or (source_short_name in available_services)

            if is_topic:
                enabled_topics.append(msg.entry)
            elif is_service:
                enabled_services.append(msg.entry)
            else:
                rospy.logwarn("Data source enable request for " + msg.entry + " is of unknown class (topic or service); ignoring")

        rospy.set_param("~lb/enabled_topics", enabled_topics)
        rospy.set_param("~lb/enabled_services", enabled_services)
    
    def enableHB(self, msg):
        # Nothing to do here but update the param server
        rospy.loginfo("HB: " + ("Enabled" if (msg.data == True) else "Disabled"))
        rospy.set_param('~hb/enabled', msg.data)

    def setHBAutoDataOffloading(self, msg):
        # Nothing to do here but update the param server
        rospy.loginfo("HB Auto Data Offloading: " + ("Enabled" if (msg.data == True) else "Disabled"))
        rospy.set_param('~hb/auto_data_offload', msg.data)

    def handleSnapshotEvent(self, req):
        enabled = rospy.get_param("~enabled", DEFAULT_ENABLED)
        # Do nothing if NEPI is not enabled
        if enabled is False:
            return

        if self.snapshot_event_handler_running is True:
            rospy.logwarn("Already running snapshot event handler, so ignoring latest event")
            return

        rospy.loginfo("Running snapshot event handler")
        self.snapshot_event_handler_running = True

        # Start onboard data saving if so configured
        onboard_save_rate_hz = rospy.get_param("~snapshot_event/onboard_save_rate_hz", DEFAULT_SNAPSHOT_SAVE_RATE_HZ)
        onboard_save_duration_s = rospy.get_param("~snapshot_event/onboard_save_duration_s", DEFAULT_SNAPSHOT_ONBOARD_SAVE_DURATION_S)
        if (onboard_save_rate_hz > 0.0 and onboard_save_duration_s > 0.0):
            rate_msg = SaveDataRate()
            rate_msg.data_product = rate_msg.ALL_DATA_PRODUCTS
            rate_msg.save_rate_hz = onboard_save_rate_hz
            self.save_data_rate_pub.publish(rate_msg)

            save_msg = SaveData()
            save_msg.save_continuous = True
            save_msg.save_raw = False
            self.save_data_pub.publish(save_msg)

            # Set a timer to disable data saving
            self.onboard_save_timer = rospy.Timer(rospy.Duration(onboard_save_duration_s), self.eventTriggeredSaveDataTimerExpired)

        triggers_lb = rospy.get_param("~snapshot_event/triggers_lb", DEFAULT_SNAPSHOT_TRIGGERS_LB)
        if triggers_lb is True:
            self.createLBDataSet()

        self.snapshot_event_handler_running = False

    def getHandledMsgTypes(self):
        # ROS gets confused by YAML dictionaries that include a '/' in the key, so we use deeper nested dictionaries in the param file, but convert them
        # to something more natural here
        handled_msg_types = {}
        msg_type_dicts = rospy.get_param('~lb/supported_msg_types', {})
        for d in msg_type_dicts:
            handled_msg_types[d['package'] + '/' + d['type']] = {'snippet_type': d['snippet_type'], \
                                                                 'conversion_call': d['conversion_call']}
        return handled_msg_types
    
    def getHandledSrvTypes(self):
        # ROS gets confused by YAML dictionaries that include a '/' in the key, so we use deeper nested dictionaries in the param file, but convert them
        # to something more natural here
        handled_srv_types = {}
        srv_type_dicts = rospy.get_param('~lb/supported_srv_types', {})        
        for d in srv_type_dicts:
            key = d['package'] + '/' + d['type']
            handled_srv_types[key] = {'snippet_type': d['snippet_type'], \
                                                        'conversion_call': d['conversion_call']}
            if 'request_args' in d:
                handled_srv_types[key]['request_args'] = d['request_args']
            
        return handled_srv_types

    def getAvailableHandledServices(self):
        while not rospy.is_shutdown():
            available_handled_services = [] 
            handled_srv_types = self.getHandledSrvTypes()
            available_services = rosservice.get_service_list()
            for service in available_services:
                service_type = rosservice.get_service_type(service) # This call is very slow, unfortunately -- hence this separate thread
                if service_type not in handled_srv_types:
                    continue
                self.available_handled_services
                available_handled_services.append(service)

            with self.available_handled_services_lock:
                self.available_handled_services = available_handled_services.copy()

            rospy.sleep(3.0)        

    def provideNEPILinkStatus(self, req):
        resp = NEPILinkStatusQueryResponse()

        # Some of the fields come directly from the nepi-bot config file. We parse it anew each time
        # because it can change via NEPI standard config or SOFTWARE channels
        resp.status.nuid = self.nepi_sdk.getBotNUID()
        resp.status.alias, resp.status.lb_comms_types = self.extractFieldsFromNEPIBotConfig()
        resp.status.public_ssh_key = self.extractPublicSSHKeyString()
        resp.status.bot_running = self.bot_running

        # Many fields come from the param server
        resp.status.enabled = rospy.get_param("~enabled", DEFAULT_ENABLED)
        resp.status.auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", DEFAULT_AUTO_ATTEMPTS_PER_HOUR)
        resp.status.log_storage_enabled = rospy.get_param("~nepi_log_storage_enabled", DEFAULT_NEPI_LOG_STORAGE_ENABLED)
        resp.status.lb_enabled = rospy.get_param("~lb/enabled", DEFAULT_LB_ENABLED)
        resp.status.lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", DEFAULT_LB_DATA_SETS_PER_HOUR)

        available_data_sources = []
        selected_data_sources = []
        base_namespace = rospy.get_namespace()

        handled_msg_types = self.getHandledMsgTypes()
        enabled_topics = rospy.get_param('~lb/enabled_topics', [])
        available_topics = rospy.get_published_topics()
        for entry in available_topics:
            msg_type = entry[1]
            if msg_type not in handled_msg_types:
                #rospy.logwarn("Debug: Skipping unhandled type " + msg_type)
                continue
            
            topic_name = entry[0]
            topic_name_short = topic_name[len(base_namespace):] if topic_name.startswith(base_namespace) else topic_name 
            
            available_data_sources.append(topic_name)
            # We only add enabled/selected items for which we can determine their topic type (not from config params), so
            # some node must already be publishing these.
            if (topic_name in enabled_topics) or (topic_name_short in enabled_topics):
                selected_data_sources.append(topic_name)
        
        # Because it is very slow to check service types, we do that asynchronously in a separate thread and just make a threadsafe copy here
        available_services = []
        with self.available_handled_services_lock:
            available_services = self.available_handled_services.copy()
        enabled_services = rospy.get_param('~lb/enabled_services', [])
        for service in available_services:
            service_name_short = service[len(base_namespace):] if service.startswith(base_namespace) else service
            available_data_sources.append(service)
            if (service in enabled_services) or (service_name_short in enabled_services):
                selected_data_sources.append(service)

        resp.status.lb_available_data_sources = available_data_sources
        resp.status.lb_selected_data_sources = selected_data_sources                
        
        resp.status.hb_enabled = rospy.get_param("~hb/enabled", DEFAULT_HB_ENABLED)
        resp.status.hb_auto_data_offloading_enabled  = rospy.get_param("~hb/auto_data_offload", DEFAULT_HB_AUTO_DATA_OFFLOAD)

        # Some fields come from the last nepi-bot status file, which we parse in runNEPIBot
        resp.status.lb_last_connection_time = self.lb_last_connection_time
        resp.status.lb_do_msg_count = self.lb_do_msg_count
        resp.status.lb_dt_msg_count = self.lb_dt_msg_count
        resp.status.hb_last_connection_time = self.hb_last_connection_time
        resp.status.hb_do_transfered_mb = self.hb_do_transfered_mb
        resp.status.hb_dt_transfered_mb = self.hb_dt_transfered_mb

        # Some fields we simply calculate
        # TODO: Should these fs query capabilities be moved to SDK?
        nepi_bot_folder = rospy.get_param('~nepi_bot_root_folder', DEFAULT_NEPI_BOT_ROOT_FOLDER)
        resp.status.lb_data_queue_size_kb = getDirectorySize(os.path.join(nepi_bot_folder, 'lb/data')) / 1000.0

        if rospy.get_param('~hb/auto_data_offload', DEFAULT_HB_AUTO_DATA_OFFLOAD) is True:
            resp.status.hb_data_queue_size_mb = getDirectorySize(rospy.get_param('~hb/data_source_folder', DEFAULT_HB_DATA_SOURCE_FOLDER)) / 1000000.0
        else:
            resp.status.hb_data_queue_size_mb = 0

        return resp

    def __init__(self):
        rospy.init_node(self.NODE_NAME)
        rospy.loginfo("Starting " + self.NODE_NAME + " node")

        self.save_cfg_if = SaveCfgIF(updateParamsCallback=None, paramsModifiedCallback=self.updateFromParamServer)

        self.auto_connect_timer = None
        self.lb_data_collection_timer = None
        self.nepi_bot_lock = threading.Lock()

        # Initialize from parameters
        self.updateFromParamServer()

        # Initialize the nepi-bot status fields
        self.lb_last_connection_time = rospy.Time()
        self.lb_do_msg_count = 0
        self.lb_dt_msg_count = 0
        self.hb_last_connection_time = rospy.Time()
        self.hb_do_transfered_mb = 0
        self.hb_dt_transfered_mb = 0
        self.bot_running = False

        # Initializer event handler fields
        self.snapshot_event_handler_running = False
        self.onboard_save_timer = None

        # Fields for very slow service type checks to run in separate thread
        self.available_handled_services = []
        self.available_handled_services_lock = threading.Lock()
        self.service_check_thread = threading.Thread(target=self.getAvailableHandledServices)
        self.service_check_thread.start()

        # Subscribe to topics
        rospy.Subscriber('~enable', Bool, self.enableNEPIEdge)
        rospy.Subscriber('~connect_now', Empty, self.connectNow)
        rospy.Subscriber('~set_auto_attempts_per_hour', Float32, self.setAutoAttemptsPerHour)
        rospy.Subscriber('~enable_nepi_log_storage', Bool, self.enableNEPIBotLogStorage)
        rospy.Subscriber('~lb/enable', Bool, self.enableLB)
        rospy.Subscriber('~lb/create_data_set_now', Empty, self.createLBDataSetNow)
        rospy.Subscriber('~lb/set_data_sets_per_hour', Float32, self.setLBDataSetsPerHour)
        rospy.Subscriber('~lb/select_data_source', StringEnable, self.selectLBDataSource)
        rospy.Subscriber('~hb/enable', Bool, self.enableHB)
        rospy.Subscriber('~hb/set_auto_data_offloading', Bool, self.setHBAutoDataOffloading)

        rospy.Subscriber('snapshot_event', Empty, self.handleSnapshotEvent)

        # Advertise services
        rospy.Service('nepi_link_status_query', NEPILinkStatusQuery, self.provideNEPILinkStatus)

        # Initialize publishers
        self.save_data_pub = rospy.Publisher('save_data', SaveData, queue_size=3)
        self.save_data_rate_pub = rospy.Publisher('save_data_rate', SaveDataRate, queue_size=3)

        # Create and initialize nepi-edge-sdk object
        self.nepi_sdk = NEPIEdgeSDK()
        nepi_bot_path = rospy.get_param('~nepi_bot_root_folder', DEFAULT_NEPI_BOT_ROOT_FOLDER)
        self.nepi_sdk.setBotBaseFilePath(nepi_bot_path)
        self.latest_nepi_status = None
        self.latest_nepi_status_lock = threading.Lock()
        self.latest_nepi_data_snippets = []
        self.nepi_data_snippets_lock = threading.Lock()

        rospy.spin()

if __name__ == '__main__':
    nepi_link_ros_bridge = NEPILinkRosBridge()
