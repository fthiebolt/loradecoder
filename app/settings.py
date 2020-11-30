#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dataCOllector agent settings app.
#
# Notes:
#
# F.Thiebolt    Apr.20  update with INFLUXDB for new datacollector app
# F.Thiebolt    Jan.20  initial release
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
MQTT_TOPICS     = [ "u4/+/+", "bu/+/+" ]    # legacy stuff
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

# MQTT metrics monitoring
MQTT_TOPICS_METRICS     = [ '$SYS/broker/load/publish/received/+',
                            '$SYS/broker/load/publish/sent/+',
                            '$SYS/broker/load/bytes/received/+',
                            '$SYS/broker/load/bytes/sent/+' ]


#
# MONGODB settings
MONGO_SERVER    = "neocampus.univ-tlse3.fr"
MONGO_PORT      = 27017

MONGO_DATABASE  = "neocampus"

# DATABASES definitions: legacy and datalake ones
MONGO_LEGACY_DB     = MONGO_DATABASE
MONGO_DATALAKE_DB   = "neocampus_datalake"

# data sensors default collections
MONGO_LEGACY_SENSOR_COLLECTION      = "measure"
#MONGO_LEGACY_SENSOR_COLLECTION      = "failedData"     # Hamdi's fourre-tout
MONGO_DATALAKE_SENSOR_COLLECTION    = "sensors"


#
# INFLUXDB settings
INFLUX_SERVER   = "neocampus.univ-tlse3.fr"
INFLUX_PORT     = 9999

INFLUX_ORG      = "neOCampus"
INFLUX_BUCKETS  = [ "sensors", "inventory" ]    # buckets to hold sensors data, sensors metadata (inventory of buildings, rooms etc)
                                                # 'sensors' bucket holds _raw_ data from sensors
                                                # 'inventory' bucket holds list of buildings, rooms, kind of sensors ...

INFLUX_BUCKET_METRICS   = "metrics"             # default bucket to hold our internal metrics

# tags for sensors bucket
INFLUX_SENSORS_BUCKET_TAGS      = ['location','building','room','kind','unitID','subID']
INFLUX_SENSORS_BUCKET_FLAGS     = ['value','value_units','value_min','value_avg','value_max','_avg_count']



#
# Aggregation settings
# (i.e downsampling)
# sensors data will get aggregated on a specified interval basis
AGGREGATION_INTERVAL    = 5     # mn

# data precision for aggregation
AGG_DATA_PRECISION      = 3

