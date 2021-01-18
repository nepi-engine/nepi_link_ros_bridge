#!/usr/bin/env python

import rospy

#from num_sdk_msgs.msg import
from num_sdk_msgs.srv import NEPIStatusQuery, NavPosQuery

from num_sdk_base.save_cfg_if import SaveCfgIF

class NEPIRosBridgeNode:
	NODE_NAME = "nepi_ros_bridge"

    def runNEPIBot(self):
        # TODO
        rospy.logwarn("nepi_ros_bridge.runNEPIBot() -- TODO")

        # Query param server for LB and HB params and launch nepi-bot

        # At completion, read the status file into class members
        # For now, we just hard-code it
        self.lb_last_connection_time = rospy.get_rostime()
        self.lb_do_msg_count += 4 # Just a canned change
        self.lb_dt_msg_count += 1 # Just a canned change
        self.hb_last_connection_time = rospy.get_rostime()
        self.hb_do_transfered_mb += 25
        self.hb_dt_transfered_mb += 10

    def createLBDataSet(self):
        # TODO
        rospy.logwarn("nepi_ros_bridge.createLBDataSet() -- TODO")

        # First, iterate through the available_data_topics (param server) to find those that are enabled
        # For each enabled topic, subscribe. The topic callback should look up the conversion function
        # and output the converted file along with generating the metadata (leveraging nepi-edge-sdk)

        # Then in this method, create the status message -- for now this will be 3DX-hardcoded, but eventually will be based on
        # a parameter mapping defined in the config file

        # Then dump status and all metadata to the LB folder (via nepi-edge-sdk)

    def updateCallbackTimer(self, timer, timer_cb, enabled, rate_per_hour):
        if enabled is True:
            if rate_per_hour > 0:
                duration_s = 3600.0 / rate_per_hour
                if timer is not None:
                    timer.shutdown()
                timer = rospy.Timer(rospy.Duration(duration_s), timer_cb)
            elif timer is not None:
                timer.shutdown()
                timer = None

    def updateAutoConnectionScheduler(self, enabled, rate_per_hour):
        self.updateCallbackTimer(self.auto_connect_timer, self.runNEPIBot, enabled, rate_per_hour)

    def updateDataCollectionScheduler(self, enabled, rate_per_hour):
        self.updateCallbackTimer(self.lb_data_collection_timer, self.createLBDataSet, enabled, rate_per_hour)

    def extractFieldsFromNEPIBotConfig(self):
        # TODO
        # Query param server for nepi-bot root folder and use that to determine nepi-bot config file
        # Open config file (read-only) and find the fields of interest

        return 9999999999, 'dummy_device', ['lb_iridium', 'lb_ip']

    def updateFromParamServer(self):
        # Most parameters are handled on an as-needed basis, so here we only
        # deal with those that control top-level behavior

        enabled = rospy.get_param("~enabled", False)
        auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", 0)
        self.updateAutoConnectionScheduler(enabled, auto_attempts_per_hour)

        lb_enabled = rospy.get_param("~lb/enabled", False)
        lb_data_sets_per_hour = rospy.get_param("~lb/data_sets_per_hour", 0)
        self.updateDataCollectionScheduler(lb_enabled, lb_data_sets_per_hour)

    def provideNEPIStatus(self, req):
        resp = NEPIStatusQueryResponse()

        # Some of the fields come directly from the nepi-bot config file. We parse it anew each time
        # because it can change via NEPI standard config or SOFTWARE channels
        resp.nuid, resp.alias, resp.lb_comms_types = self.extractFieldsFromNEPIBotConfig()

        # Many fields come from the param server
        resp.enabled = rospy.get_param("~enabled", False)
        resp.auto_attempts_per_hour = rospy.get_param("~auto_attempts_per_hour", 0)
        resp.lb_enabled = rospy.get_param("~lb/enabled", False)
        for entry in rospy.get_param("~lb/available_data_sources", None):
            if hasattr(entry, "topic"):
                resp.lb_available_data_sources.append(entry.topic)
            elif hasattr(entry, "service"):
                resp.lb_available_data_sources.append(entry.service)
            if topic_desc.enabled is True:
                resp.lb_selected_data_sources.append(topic_desc.topic)
        resp.lb_max_data_queue_size_mb = rospy.get_param("~max_data_queue_size_mb", 20)
        resp.hb_enabled = rospy.get_param("~hb/enabled", False)
        resp.hb_auto_data_offloading_enabled  = rospy.get_param("~hb/auto_data_offload", False)

        # Some fields come from the last nepi-bot status file, which we parse in runNEPIBot
        resp.lb_last_connection_time = self.lb_last_connection_time
        resp.lb_do_msg_count = self.lb_do_msg_count
        resp.lb_dt_msg_count = self.lb_dt_msg_count
        resp.hb_last_connection_time = self.hb_last_connection_time
        resp.hb_do_transfered_mb = self.hb_do_transfered_mb
        resp.hb_dt_transfered_mb = self.hb_dt_transfered_mb

        return resp

    def __init__(self):
		rospy.loginfo("Starting " + self.NODE_NAME + "node")
		rospy.init_node(self.NODE_NAME)

        self.save_cfg_if = SaveCfgIF(updateParamsCallback=self.updateParamsOnServer, paramsModifiedCallback=self.updateFromParamServer)

        self.auto_connect_timer = None
        self.lb_data_collection_timer = None

        # Initialize from parameters
        self.updateFromParamServer()

        # Initialize the nepi-bot status fields
        self.lb_last_connection_time = rospy.Time()
        self.lb_do_msg_count = 0
        self.lb_dt_msg_count = 0
        self.hb_last_connection_time = rospy.Time()
        self.hb_do_transfered_mb = 0
        self.hb_dt_transfered_mb = 0

        # Advertise services
        rospy.Service('nepi_status_query', NEPIStatuQuery, self.provide_nepi_status)
