#!/usr/bin/env python
import threading

import rospy

from std_msgs.msg import Bool, Empty, Float32
from num_sdk_msgs.msg import StringArray
from num_sdk_msgs.srv import NEPIStatusQuery, NEPIStatusQueryResponse

from num_sdk_base.save_cfg_if import SaveCfgIF

class NEPIEdgeRosBridge:
    NODE_NAME = "nepi_edge_ros_bridge"

    def autoConnectTimerExpired(self, timer):
        self.runNEPIBot()

    def runNEPIBot(self):
        # TODO
        rospy.logwarn("nepi_ros_bridge.runNEPIBot() -- TODO")

        # Query param server for LB and HB params and launch nepi-bot

        # Ensure this only runs once at a time -- this lock is held through the entire execution of nepi-bot
        #if self.nepi_bot_lock.acquire(blocking=False) is True: # Python 3
        if self.nepi_bot_lock.acquire(False) is True:
            # At completion, read the status file into class members
            # For now, we just hard-code it
            self.lb_last_connection_time = rospy.get_rostime()
            self.lb_do_msg_count += 4 # Just a canned change
            self.lb_dt_msg_count += 1 # Just a canned change
            self.hb_last_connection_time = rospy.get_rostime()
            self.hb_do_transfered_mb += 25
            self.hb_dt_transfered_mb += 10

            # Must ALWAYS release the lock
            self.nepi_bot_lock.release()

        else: # Unable to acquire
            rospy.logwarn("Unable to run nepi-bot because it is already running in another thread")

    def createLBDataSet(self, timer):
        # TODO
        rospy.logwarn("nepi_ros_bridge.createLBDataSet() -- TODO")

        # First, iterate through the available_data_topics (param server) to find those that are enabled
        # For each enabled topic, subscribe. The topic callback should look up the conversion function
        # and output the converted file along with generating the metadata (leveraging nepi-edge-sdk)

        # Then in this method, create the status message -- for now this will be 3DX-hardcoded, but eventually will be based on
        # a parameter mapping defined in the config file

        # Then dump status and all metadata to the LB folder (via nepi-edge-sdk)

    def updateAutoConnectionScheduler(self, enabled, rate_per_hour):
        if (enabled is True) and (rate_per_hour > 0.0):
            rospy.loginfo("Enabling auto connect scheduler")
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
            rospy.loginfo("Enabling LB data collection scheduler")
            duration_s = 3600.0 / rate_per_hour
            if self.lb_data_collection_timer is not None:
                self.lb_data_collection_timer.shutdown()
            self.lb_data_collection_timer = rospy.Timer(rospy.Duration(duration_s), self.createLBDataSet)
        elif self.lb_data_collection_timer is not None:
            rospy.logwarn('Disabling LB data collection scheduler')
            self.lb_data_collection_timer.shutdown()
            self.lb_data_collection_timer = None
        # Otherwise, the time is already shut down

    def extractFieldsFromNEPIBotConfig(self):
        # TODO
        # Query param server for nepi-bot root folder and use that to determine nepi-bot config file
        # Open config file (read-only) and find the fields of interest

        return '9999999999', 'dummy_device', ['lb_iridium', 'lb_ip']

    def updateFromParamServer(self):
        # Most parameters are handled on an as-needed basis, so here we only
        # deal with those that control top-level behavior

        enabled = rospy.get_param("~enabled", False)
        auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", 0.0)
        self.updateAutoConnectionScheduler(enabled, auto_attempts_per_hour)

        # Only apply the data_sets_per_hour if top-level enabled is true
        lb_enabled = rospy.get_param("~lb/enabled", False)
        lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", 0)
        run_data_collection = enabled and lb_enabled
        self.updateDataCollectionScheduler(run_data_collection, lb_data_sets_per_hour)

    def enableNEPIEdge(self, msg):
        # Check if this is truly an update -- if not, don't do anything
        prev_enabled = rospy.get_param('~enabled', False)
        new_enabled = msg.data
        if prev_enabled != new_enabled:
            rospy.logwarn("Setting NEPI Edge enabled to " + str(new_enabled))
            # Update the param server
            rospy.set_param("~enabled", new_enabled)

            # Now just reinitialize everything as if we were just starting up with this value
            self.updateFromParamServer()

    def connectNow(self, msg):
        enabled = rospy.get_param("~enabled", False)
        if enabled is True:
            # Simply a pass-through to runNEPIBot()
            self.runNEPIBot()
        else:
            rospy.logwarn("Cannot connect now -- NEPI Edge is currently disabled")

    def setAutoAttemptsPerHour(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_auto_attempts_per_hour = rospy.get_param('~auto_attempts_per_hour')
        new_auto_attempts_per_hour = msg.data
        if prev_auto_attempts_per_hour != new_auto_attempts_per_hour:
            # Update the param server
            rospy.set_param('~auto_attempts_per_hour', new_auto_attempts_per_hour)
            # Update the scheduler
            enabled = rospy.get_param('~enabled', False)
            self.updateAutoConnectionScheduler(enabled, new_auto_attempts_per_hour)

    def enableLB(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_lb_enabled = rospy.get_param('~lb/enabled', False)
        new_lb_enabled = msg.data
        if prev_lb_enabled != new_lb_enabled:
            # Update the param server
            rospy.set_param('~lb/enabled', new_lb_enabled)
            # Update the scheduler
            lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", 0.0)
            self.updateDataCollectionScheduler(new_lb_enabled, lb_data_sets_per_hour)

    def setLBDataSetsPerHour(self, msg):
        # Check if this is truly an update -- if not, don't need to do anything
        prev_sets_per_hour = rospy.get_param('~lb/data_sets_per_hour', 0.0)
        new_sets_per_hour = msg.data
        if prev_sets_per_hour != new_sets_per_hour:
            # Update the param server
            rospy.set_param('~lb/data_sets_per_hour', new_sets_per_hour)
            # Update the scheduler
            lb_enabled = rospy.get_param('~lb/enabled', False)
            self.updateDataCollectionScheduler(lb_enabled, new_sets_per_hour)

    def selectLBDataSources(self, msg):
        available_sources = rospy.get_param("~lb/available_data_sources")
        # first set all sources as disabled -- we will individually enable them below
        for source in available_sources:
            source['enabled'] = False

        for selected in msg.entries:
            selected_identified = False
            for available in available_sources:
                if 'topic' in available:
                    if available['topic'] == selected:
                        available['enabled'] = True
                        selected_identified = True
                        break
                elif 'service' in available:
                    if available['service'] == selected:
                        available['enabled'] = True
                        selected_identified = True
                        break
            # Now ensure that we found the selected entry
            if selected_identified is False:
                rospy.logwarn(selected + " does not appear to be an available LB data source... cannot enable")
            else:
                rospy.loginfo("Enabling LB data source: " + selected)

        # Now write back the updated sources
        rospy.set_param("~lb/available_data_sources", available_sources)

    def setLBMaxDataQueueSize(self, msg):
        # Nothing to do here but update the param server
        rospy.set_param('~lb/max_data_queue_size_mb', msg.data)

    def enableHB(self, msg):
        # Nothing to do here but update the param server
        rospy.set_param('~hb/enabled', msg.data)

    def setHBAutoDataOffloading(self, msg):
        # Nothing to do here but update the param server
        rospy.set_param('~hb/auto_data_offload', msg.data)

    def provideNEPIStatus(self, req):
        resp = NEPIStatusQueryResponse()

        # Some of the fields come directly from the nepi-bot config file. We parse it anew each time
        # because it can change via NEPI standard config or SOFTWARE channels
        resp.status.nuid, resp.status.alias, resp.status.lb_comms_types = self.extractFieldsFromNEPIBotConfig()

        # Many fields come from the param server
        resp.status.enabled = rospy.get_param("~enabled", False)
        resp.status.auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", 0)
        resp.status.lb_enabled = rospy.get_param("~lb/enabled", False)
        for entry in rospy.get_param("~lb/available_data_sources", None):
            if 'topic' in entry:
                resp.status.lb_available_data_sources.append(entry['topic'])
            elif 'service' in entry:
                resp.status.lb_available_data_sources.append(entry['service'])
            if entry['enabled'] is True:
                resp.status.lb_selected_data_sources.append(entry['topic'])
        resp.status.lb_max_data_queue_size_mb = rospy.get_param("~lb/max_data_queue_size_mb", 20)
        resp.status.hb_enabled = rospy.get_param("~hb/enabled", False)
        resp.status.hb_auto_data_offloading_enabled  = rospy.get_param("~hb/auto_data_offload", False)

        # Some fields come from the last nepi-bot status file, which we parse in runNEPIBot
        resp.status.lb_last_connection_time = self.lb_last_connection_time
        resp.status.lb_do_msg_count = self.lb_do_msg_count
        resp.status.lb_dt_msg_count = self.lb_dt_msg_count
        resp.status.hb_last_connection_time = self.hb_last_connection_time
        resp.status.hb_do_transfered_mb = self.hb_do_transfered_mb
        resp.status.hb_dt_transfered_mb = self.hb_dt_transfered_mb

        return resp

    def __init__(self):
        rospy.loginfo("Starting " + self.NODE_NAME + "node")
        rospy.init_node(self.NODE_NAME)

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

        # Subscribe to topics
        rospy.Subscriber('~enable', Bool, self.enableNEPIEdge)
        rospy.Subscriber('~connect_now', Empty, self.connectNow)
        rospy.Subscriber('~set_auto_attempts_per_hour', Float32, self.setAutoAttemptsPerHour)
        rospy.Subscriber('~lb/enable', Bool, self.enableLB)
        rospy.Subscriber('~lb/set_data_sets_per_hour', Float32, self.setLBDataSetsPerHour)
        rospy.Subscriber('~lb/select_data_sources', StringArray, self.selectLBDataSources)
        rospy.Subscriber('~lb/set_max_data_queue_size_mb', Float32, self.setLBMaxDataQueueSize)
        rospy.Subscriber('~hb/enable', Bool, self.enableHB)
        rospy.Subscriber('~hb/set_auto_data_offloading', Bool, self.setHBAutoDataOffloading)

        # Advertise services
        rospy.Service('~nepi_status_query', NEPIStatusQuery, self.provideNEPIStatus)

        rospy.spin()

if __name__ == '__main__':
    nepi_edge_ros_bridge = NEPIEdgeRosBridge()
