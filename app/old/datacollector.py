#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# dataCOllector
#
# dataCOllectors are the only applications allowed to push data sensors to
# the neOCampus database.
#
# This new version will continue to grab data from MQTT broker but this time,
# it will send data to InfluxDB.
#
# F.Thiebolt    apr.20  initial release
#



# #############################################################################
#
# Import zone
#
import os
import sys
import signal
import time
import json
from datetime import datetime
import threading

# Logging
import logging
# InfluxDB client --> https://pypi.org/project/influxdb-client/
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

# Database facility
from database.influxModule import InfluxModule

# MQTT facility
from comm.mqttConnect import CommModule

# settings
import settings



# #############################################################################
#
# Global variables
# (scope: this file)
#

_condition          = None  # conditional variable used as interruptible timer
_shutdownEvent      = None  # signall across all threads to send stop event



# #############################################################################
#
# Functions
#

#
# Function ctrlc_handler
def ctrlc_handler(signum, frame):
    global _shutdownEvent
    print("<CTRL + C> action detected ...");
    # activate shutdown mode
    assert _shutdownEvent!=None
    _shutdownEvent.set()
    # ... and notify to timer
    try:
        _condition.acquire()
        _condition.notify()
        _condition.release()
    except Exception as ex:
        pass



# #############################################################################
#
# MAIN
#
def main():

    # Global variables
    global _shutdownEvent

    # create threading.event
    _shutdownEvent = threading.Event()

    # Trap CTRL+C (kill -2)
    signal.signal(signal.SIGINT, ctrlc_handler)


    #
    # Database
    log.info("Initiate connection to InfluxDB database ...")

    params = dict()
    
    # shutown master event
    params['_shutdownEvent'] = _shutdownEvent

    # simulation mode (i.e read-only mode)
    params['sim'] = settings.SIM

    # influx_token
    _influx_token = os.getenv("INFLUX_TOKEN")
    if( _influx_token is None or _influx_token == "" ):
        log.error("unspecified INFLUX_TOKEN ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['influx_token'] = _influx_token

    # influx_server
    _influx_server = os.getenv("INFLUX_SERVER", settings.INFLUX_SERVER)
    if( _influx_server is None or _influx_server == "" ):
        log.error("unspecified INFLUX_SERVER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['influx_server'] = _influx_server

    # influx_port
    _influx_port = os.getenv("INFLUX_PORT", settings.INFLUX_PORT)
    if( _influx_port is None or _influx_port == "" ):
        log.error("unspecified INFLUX_PORT ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['influx_port'] = _influx_port

    # influx_org
    _influx_org = os.getenv("INFLUX_ORG", settings.INFLUX_ORG)
    if( _influx_org is None or _influx_org == "" ):
        log.error("unspecified INFLUX_ORG (i.e organization) ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['influx_org'] = _influx_org

    # influx_buckets
    try:
        _influx_buckets = json.loads(os.getenv("INFLUX_BUCKETS"))
    except Exception as ex:
        # failed to find env var INFLUX_BUCKETS ... load from settings
        _influx_buckets = settings.INFLUX_BUCKETS
    if( _influx_buckets is None or not len(_influx_buckets) ):
        log.error("unspecified or empty INFLUX_BUCKETS ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['bucket_sensors']    = _influx_buckets[0]
    if( len(_influx_buckets) >= 2 ):
        params['bucket_inventory']  = _influx_buckets[1]

    dbclient = None
    try:
        # init client ...
        dbclient = InfluxModule( **params )

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to instantiate database module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to instantiate database module: " + str(ex))
        time.sleep(3)
        sys.exit(1)


    #
    # MQTT
    log.info("Instantiate MQTT communications module ...")

    params = dict()
    
    # shutown master event
    params['_shutdownEvent'] = _shutdownEvent

    # simulation mode (i.e read-only mode)
    params['sim'] = settings.SIM

    # credentials
    _mqtt_user = os.getenv("MQTT_USER", settings.MQTT_USER)
    if( _mqtt_user is None or not len(_mqtt_user) ):
        log.error("unspecified MQTT_USER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['mqtt_user'] = _mqtt_user

    _mqtt_passwd = os.getenv("MQTT_PASSWD", settings.MQTT_PASSWD)
    if( _mqtt_passwd is None or not len(_mqtt_passwd) ):
        log.error("unspecified MQTT_PASSWD ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['mqtt_passwd'] = _mqtt_passwd

    # topics to subscribe and addons
    try:
        _mqtt_topics = json.loads(os.getenv("MQTT_TOPICS"))
    except Exception as ex:
        # failed to find env var MQTT_TOPICS ... load from settings
        _mqtt_topics = settings.MQTT_TOPICS
    if( _mqtt_topics is None or not len(_mqtt_topics) ):
        log.error("unspecified or empty MQTT_TOPICS ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['mqtt_topics'] = _mqtt_topics

    # host'n port parameters
    params['mqtt_server'] = os.getenv("MQTT_SERVER", settings.MQTT_SERVER)
    params['mqtt_port'] = os.getenv("MQTT_PORT", settings.MQTT_PORT)

    # unitID
    params['unitID'] = os.getenv("MQTT_UNITID", settings.MQTT_UNITID)

    # TODO: replace with log.debug( ... )
    if getLogLevel().lower() == "debug":
        print(params)

    client = None
    try:
        # init client ...
        client = CommModule( **params )

        # register own message handler
        client.handle_message = dbclient.msgHandler

        # ... then start client :)
        client.start()

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to start MQTT comm module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to start MQTT comm module: " + str(ex))
        time.sleep(3)
        sys.exit(1)


    #
    # main loop

    # initialise _condition
    _condition = threading.Condition()

    with _condition:

        while( not _shutdownEvent.is_set() ):

            #
            #
            # ADD CUSTOM PROCESSING HERE
            #

            # now sleeping till next event
            if( _condition.wait( 2.0 ) is False):
                #log.debug("timeout reached ...")
                pass
            else:
                log.debug("interrupted ... maybe a shutdown ??")
                time.sleep(1)

    # end of main loop
    log.info("app. is shutting down ... have a nice day!")
    _shutdownEvent.set()
    time.sleep(4)

    # delete objects
    del client
    client = None
    del dbclient
    dbclient = None


# Execution or import
if __name__ == "__main__":

    #
    print("\n###\nneOCampus dataCOllector app.\n###")

    # defined debug mode ?
    if( os.getenv("DEBUG")=='1' or os.getenv("DEBUG") is True ):
        log.info("DEBUG mode activation ...")
        setLogLevel( logging.DEBUG )
        # print all environment variables
        print(os.environ)

    # SIMULATION mode ?
    if( os.getenv("SIM")=='1' or os.getenv("SIM") is True ):
        log.info("SIMULATION mode activated ...")
        settings.SIM = True
        time.sleep(1)

    #sys.exit(0)

    # Start main app.
    main()


# The END - Jim Morrison 1943 - 1971
#sys.exit(0)

