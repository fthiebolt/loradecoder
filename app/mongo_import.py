#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus MongoDB import to InfluxDB
#
# This application will pull data from MongoDB to push them to InfluxDB.
# It accepts the following options:
#
#   --legacy        will import from mongoDB 'neocampus' database and will look
#                   for legacy data (2016 ~ 2020) ---Hamdi's data
#   --src=<name>    will import from mongoDB 'datalake' database and will look
#                   for '<name>' and '<name>_index' collections
#                   e.g 
#
#
# Note: there won't be any problem importing two-times the same data from
#   MongoDB because they will feature the same group Tag with the same timestamp
#   hence InfluxDB will overwrite. However, since timestamps are locally generated
#   (i.e at the time data is received), we need to carefully check about data that
#   have been grabbed from MQTT by our dataCOllector ... because same data will
#   exist in MongoDB so we need to consider a few seconds interval.
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

# CLI options
import argparse

# Logging
import logging
# InfluxDB client --> https://pypi.org/project/influxdb-client/
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

#
# --- project related imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

# Database facility
from database.influxModule import InfluxModule
from database.mongoModule import MongoModule

# Data importer algorithms
from offline_import.legacy_processor import legacyProcessor
#from offline_import.sge_processor import sgeProcessor

# settings
import settings



# #############################################################################
#
# Global variables
# (scope: this file)
#

# Variable to store commandline arguments
ARGS                = None

_condition          = None  # conditional variable used as interruptible timer
_shutdownEvent      = None  # signall across all threads to send stop event



# #############################################################################
#
# Functions
#

#
# Function ctrlc_handler
def ctrlc_handler(signum, frame):
    global _shutdownEvent, _condition
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
    global ARGS, _shutdownEvent, _condition

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

    _mongo_user = os.getenv("MONGO_USER")
    if( _mongo_user is None or _mongo_user == "" ):
        log.error("unspecified MONGO_USER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['user'] = _mongo_user
    
    _mongo_passwd = os.getenv("MONGO_PASSWD")
    if( _mongo_passwd is None or _mongo_passwd == "" ):
        log.error("unspecified MONGO_PASSWD ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['passwd'] = _mongo_passwd

    _mongo_server = os.getenv("MONGO_SERVER", settings.MONGO_SERVER)
    if( _mongo_server is None or _mongo_server == "" ):
        log.error("unspecified MONGO_SERVER ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['server'] = _mongo_server

    _mongo_port = os.getenv("MONGO_PORT", settings.MONGO_PORT)
    if( _mongo_port is None or _mongo_port == "" ):
        log.error("unspecified MONGO_PORT ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['port'] = _mongo_port

    # select mongo database
    if( ARGS.legacy is True ):
        _mongo_database = settings.MONGO_LEGACY_DB
        log.debug("\t[LEGACY] importing LEGACY data from mongoDB database '%s'" % str(_mongo_database) )
    else:
        _mongo_database = settings.MONGO_DATALAKE_DB
    if( _mongo_database is None or _mongo_database == "" ):
        log.error("unspecified MONGO DATABASE ?!?! ... aborting")
        time.sleep(3)
        sys.exit(1)
    params['database'] = _mongo_database


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


    influxClient = None
    try:
        # init client ...
        influxClient = InfluxModule( **params )

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to instantiate database module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to instantiate database module: " + str(ex))
        time.sleep(3)
        sys.exit(1)


    #
    # dataImport module
    log.info("Instantiate dataImport module ...")

    params = dict()
    
    # shutown master event
    params['_shutdownEvent'] = _shutdownEvent

    # simulation mode (i.e read-only mode)
    params['sim'] = settings.SIM

    # SOURCE client
    params['src'] = mongoClient

    # TARGET client
    params['dst'] = influxClient

    # name of source collection: None --> default one to get selected by importer
    if( ARGS.collection_srcName is not None ):
        params['collection_name'] = ARGS.collection_srcName
    elif( ARGS.legacy is True ):
        params['collection_name'] = settings.MONGO_LEGACY_SENSOR_COLLECTION
    else:
        params['collection_name'] = settings.MONGO_DATALAKE_SENSOR_COLLECTION

    # optional start_date
    if( ARGS.start_date is not None ):
        params['start_date'] = ARGS.start_date

    # optional end_date
    if( ARGS.end_date is not None ):
        params['end_date'] = ARGS.end_date

    # optional start_id
    if( ARGS.start_id is not None ):
        params['start_id'] = ARGS.start_id

    # optional disable duplicate checking
    if( ARGS.disableDuplicateCheck is True ):
        params['disableDuplicateCheck'] = ARGS.disableDuplicateCheck


    client = None
    try:
        # init client ...
        if( ARGS.legacy is True ):
            client = legacyProcessor( **params )
        elif( ARGS.collection_srcName.lower() == "sge" ):
            client = sgeProcessor( **params )
        else:
            raise Exception("'%s' has no data import processor known ... aborting") % str(ARGS.collection_srcName)

        # ... then start client :)
        client.start()

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to start data importer module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to start data importer module: " + str(ex))
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

            print("\t[PROCESSING STILL ON WAY] ...")


    # end of main loop
    log.info("app. is shutting down ... have a nice day!")
    _shutdownEvent.set()
    time.sleep(4)

    # delete objects
    del client
    client = None
    del influxClient
    influxClient = None
    del mongoClient
    mongoClient = None


#
# Execution or import
if __name__ == "__main__":

    #
    print("\n###\nneOCampus mongoDB --> influxDB importer app.\n###")

    # Parse arguments
    parser = argparse.ArgumentParser( allow_abbrev=False,
                         description="neOCampus mongoDB --> influxDB import app." )

    # legacy mode --> neOCampus legacy database (i.e 'neocampus'),
    # ... otherwise neOCampus datalake database (i.e 'neocampus_datalake').
    parser.add_argument( '--legacy', action="store_true",
                        help="legacy data import from 'neocampus' database (Hamdi's data).\
Otherwise it will implicitely select 'neocampus_datalake'." )

    # <NAME> collection import
    parser.add_argument( '--src', metavar='collection_srcName',
                        dest="collection_srcName", type=str, nargs='?',
                        help="data import from '<name>' collection." )

    # RFC-3339 start date
    parser.add_argument( '--start-date', metavar='start_date',
                        dest="start_date", type=str, nargs='?',
                        help="RFC-3339 UTC start_date: data import will start from this date." )

    # RFC-3339 end date
    parser.add_argument( '--end-date', metavar='end_date',
                        dest="end_date", type=str, nargs='?',
                        help="RFC-3339 UTC end_date: data import process won't go over this date. \
exemple: --end-date=2020-04-09T19:00:00Z" )

    # start _id
    parser.add_argument( '--start-id', metavar='start_date',
                        dest="start_id", type=str, nargs='?',
                        help="mongoDB _id to start from: data import will start from this _id" )

    # disableDuplicateCheck
    parser.add_argument( '--disableDuplicateCheck', action="store_true",
                        help="Force disabling for duplicate detection mechanism while inserting data. \
WARNING: you ought to be aware of what you're doing: \
    locally stamped data may conflict with existing ones ..." )

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

