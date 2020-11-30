#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# live collector : dataCOllector for live (i.e mqtt) data
#
# dataCOllectors are the only applications allowed to push data sensors to
# the neOCampus databases.
#
# This new version will continue to grab data from MQTT broker but this time,
#   it will send data to BOTH InfluxDB AND MongoDB.
# Hence, to maintain consistency betwwen live data ingested in influxDB and
# the same data ingested in mongoDB, we'll use the same timestamp:
#   - either data is itself timestamped (e.g within payload or any additional field)
#   - either live_collector add a uniq timestamp and send it along the payload and
#       topic to both msgHandlers at mongoDB and influxDB
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
from datetime import datetime,timezone
import threading

# CLI options
import argparse

# Logging
import logging

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

# Database facility
from database.influxModule import InfluxModule
from database.mongoModule import MongoModule

# MQTT facility
from comm.mqttConnect import CommModule

# settings
import settings



# #############################################################################
#
# Global variables
# (scope: this file)
#

# Variable to store commandline arguments
ARGS                = None

mongoClient         = None  # mongoDB client
influxClient        = None  # influcDB client

_condition          = None  # conditional variable used as interruptible timer
_shutdownEvent      = None  # signall across all threads to send stop event



# #############################################################################
#
# Functions
#

#
# Function ctrlc_handler
def ctrlc_handler(signum, frame):
    global _shutdownEvent,_condition
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


#
# Decorator to mshHandler function
def _myMsgHandler( func ):
    ''' Handle MQTT messages with additional stuff
    '''
    def _wrapper( *args, **kwargs ):
        kwargs['targetDB'] = [ mongoClient, influxClient ]

        # call to function
        func( *args, **kwargs )

    return _wrapper


#
# Function to handle MQTT messages
@_myMsgHandler 
def myMsgHandler( topic, payload, *args, **kwargs ):
    ''' function called whenever our MQTT client receive weather data.
        Beware that it's called by mqtt_loop's thread !
    '''

    if( kwargs.get('targetDB') is None ): return

    # add UTC timestamp if not already in payload
    kwargs['timestamp'] = datetime.now(timezone.utc)

    # force 'no duplicate check' ... because we just generated the timestamp
    kwargs['forceNoDuplicateCheck'] = True

    # call msgHandler from all targets
    try:
        res = False
        for _db in kwargs.get('targetDB'):
            if( _db is None ): continue
            try:
                # call DB specific msgHandler
                if( _db.msgHandler( topic, payload, *args, **kwargs ) is True ):
                    res = True
            except Exception as ex:
                log.error("hum hum, something went wrong while calling myMsgHandler: " + str(ex) )
                time.sleep(1.2)
                continue
        if( res ):
            _measureTime = kwargs['timestamp'].replace(microsecond=0,tzinfo=None).isoformat()+'Z'
            print(f"{_measureTime}  Topic: {topic:>32}  Payload: {payload}" )
    except Exception as ex:
        log.error("hum hum, something went wrong: " + str(ex) )
        time.sleep(1.2)
        
    return



# #############################################################################
#
# MAIN
#
def main():

    # Global variables
    global ARGS, _shutdownEvent, _condition, mongoClient, influxClient

    # create threading.event
    _shutdownEvent = threading.Event()

    # Trap CTRL+C (kill -2)
    signal.signal(signal.SIGINT, ctrlc_handler)


    #
    # mongoDB database client
    log.info("Initiate connection to MongoDB database ...")

    params = dict()
    
    # shutown master event
    params['_shutdownEvent'] = _shutdownEvent

    # simulation mode (i.e read-only mode)
    params['sim'] = settings.SIM

    # mongo_user
    _mongo_user = os.getenv("MONGO_USER")
    if( _mongo_user is None or _mongo_user == "" ):
        log.error("unspecified MONGO_USER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['user'] = _mongo_user
    
    # mongo_passwd
    _mongo_passwd = os.getenv("MONGO_PASSWD")
    if( _mongo_passwd is None or _mongo_passwd == "" ):
        log.error("unspecified MONGO_PASSWD ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['passwd'] = _mongo_passwd

    # mongo_server
    _mongo_server = os.getenv("MONGO_SERVER", settings.MONGO_SERVER)
    if( _mongo_server is None or _mongo_server == "" ):
        log.error("unspecified MONGO_SERVER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['server'] = _mongo_server

    # mongo_port
    _mongo_port = os.getenv("MONGO_PORT", settings.MONGO_PORT)
    if( _mongo_port is None or _mongo_port == "" ):
        log.error("unspecified MONGO_PORT ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['port'] = _mongo_port

    # mongo_database
    _mongo_database = settings.MONGO_DATALAKE_DB
    if( _mongo_database is None or _mongo_database == "" ):
        log.error("unspecified MONGO DATABASE ?!?! ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['database'] = _mongo_database

    # default mongo_collection
    _mongo_collection = settings.MONGO_DATALAKE_SENSOR_COLLECTION
    if( _mongo_collection is None or _mongo_collection == "" ):
        log.error("unspecified DEFAULT MONGO COLLECTION ?!?! ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['collection_name'] = _mongo_collection


    mongoClient = None
    try:
        # init client ...
        mongoClient = MongoModule( **params )

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to instantiate database module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to instantiate database module: " + str(ex))
        time.sleep(3)
        sys.exit(1)


    #
    # influxDB database client
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


    try:
        # init client ...
        if( ARGS.disable_influxdb is True ):
            log.debug("influxDB client disabled ...")
            influxClient = None
        else:
            # create influxDB client
            influxClient = InfluxModule( **params )

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

    # mqtt user
    _mqtt_user = os.getenv("MQTT_USER", settings.MQTT_USER)
    if( _mqtt_user is None or not len(_mqtt_user) ):
        log.error("unspecified MQTT_USER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['mqtt_user'] = _mqtt_user

    # mqtt password
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
        log.warning("MQTT_TOPICS env.var failed to load :|")
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

    # debug ??
    if getLogLevel().lower() == "debug":
        log.debug(params)


    client = None
    try:
        # init client ...
        client = CommModule( **params )

        # register own message handler
        client.handle_message = myMsgHandler

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

            # processing still on way ?
            if( client.is_alive() is not True ):
                break

            log.debug("\t[PROCESSING STILL ON WAY] ...")


    # end of main loop
    log.info("app. is shutting down ... have a nice day!")
    _shutdownEvent.set()
    time.sleep(4)

    # delete objects
    del client
    client = None
    del mongoClient
    mongoClient = None
    del influxClient
    influxClient = None


#
# Execution or import
if __name__ == "__main__":

    #
    print("\n###\nneOCampus live collector app.\n###")

    # Parse arguments
    parser = argparse.ArgumentParser( allow_abbrev=False,
                         description="neOCampus live data collector --> [ mongoDB, influxDB ] app." )
    # disable influxdb
    parser.add_argument( '--disable-influxdb', action="store_true",
                        help="disable influxDB storage." )

    # simulation mode
    parser.add_argument( '--sim', action="store_true",
                        help="activate simulation mode, i.e no write in database nor mqtt nor ..." )
    # debug mode
    parser.add_argument( '-d', '--debug', action="store_true",
                        help="Enable debug mode (default is False)." )

    ARGS = parser.parse_args()
    #print("\nARGUMENTS:\n" + str(ARGS) + "\n"); sys.exit(0)

    # defined debug mode ?
    if( ARGS.debug is True or os.getenv("DEBUG")=='1' or os.getenv("DEBUG") is True ):
        log.info("DEBUG mode activation ...")
        setLogLevel( logging.DEBUG )
        # print all environment variables
        print(os.environ)

    # SIMULATION mode ?
    if( ARGS.sim is True or os.getenv("SIM")=='1' or os.getenv("SIM") is True ):
        log.info("SIMULATION mode activated ...")
        settings.SIM = True
        time.sleep(1)

    #sys.exit(0)

    # Start main app.
    main()


# The END - Jim Morrison 1943 - 1971
#sys.exit(0)

