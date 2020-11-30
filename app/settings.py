#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# loradecoder app. settings
#
# Notes:
#
# F.Thiebolt    Nov.20  initial release
#



# #############################################################################
#
# Import zone
#

# logs
import logging



# #############################################################################
#
# Global variables
#


#
# Simulation mode: read-only database, files and so on ...
SIM = False


#
# Log (default value)
LOG_LEVEL = logging.INFO
#LOG_LEVEL = logging.DEBUG


#
# MQTT settings
MQTT_SERVER     = "neocampus.univ-tlse3.fr"
MQTT_PORT       = 1883

MQTT_KEEP_ALIVE         = 60    # set accordingly to the mosquitto server setup
MQTT_RECONNECT_DELAY    = 7     # minimum delay before retrying to connect (max. is 120 ---paho-mq  defaults)

MQTT_USER       = ''
MQTT_PASSWD     = ''

# input topics for data (i.e subscribe)
MQTT_TOPICS     = [ "TestTopic/lora/#" ]    # legacy stuff
#MQTT_TOPICS     = [ "#" ]           # allowed to subscribe to all ... but carefull filters required ;)

# unitID enables identity of a neOCampus client. When subscribing to topipcs, incoming messages
# will get filtered whenever there's a matching between destID (of msg) == unitID
# or if destID=="all". unitID="None" means that there won't be any filter to the incoming messages.
MQTT_UNITID     = None  # we're a reader, hence we accept all messages

# data precision
# floating point data will get rounded up to <xx> digits
MQTT_DATA_PRECISION     = 2

# possible timestamp keys in payload
MQTT_PAYLOAD_TIMESTAMPS = [ 'datatime', 'timestamp', 'time' ]

