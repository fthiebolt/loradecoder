#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# sensors'data aggregation (i.e downsampling)
#
# Goal of this application is to produce sensors downsampling data: instead
# of parsing tons of data to generate the same view again and again and again,
# we create the following buckets:
#   - sensors_hires:    per sensor 5mn resolution
#   - sensors_lowres:   per sensor 1day resolution
# _____________________________________________________________________________
# Note:
# according to 'sensors_aggregator.log.keep', we found that:
#   - output is splitted into several tables
#   - in each table, there's a table IDX that is the same for a sensor groupKey
#       (i.e the same sensor)
#   - sensors values are not splitted across various tables
#
# ==> hence synthesis is just a matter of going through all tables and to keep
#   track of the table IDX
# _____________________________________________________________________________
# TODO:
#   - nothing up to now :)
# _____________________________________________________________________________
#
# F.Thiebolt    jun.20  moved to hires and lowres sensors buckets
# F.Thiebolt    jun.20  initial release
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
from datetime import datetime, timedelta
import calendar
import threading

# CLI options
import argparse

# Logging
import logging
# InfluxDB client --> https://pypi.org/project/influxdb-client/
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# pandas, numpy
import pandas as pd
import numpy as np

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

# Database facility
from database.influxModule import InfluxModule

# Aggregation facility
from downsampling.sensor_aggregator import getSensorsData, sensor_aggregator, writeAgg, updateAggDay, getAggLastIndex   # updateAggMonth
from downsampling.helpers import pprint_aggregation

# settings
import settings



# #############################################################################
#
# Global variables
# (scope: this file)
#

MAX_RETRIES         = 3     # maximum number of errors allowed before restarting the whole app.


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
        log.warning("unable to acquire _condition ?!?! ..")
        pass



#
# Function align to next INTERVAL_mn event
def nextAggEvent( cur_date=None ):

    if( cur_date is None ):
        _cur_date = datetime.utcnow()
    else:
        _cur_date = cur_date

    _cur_date = _cur_date.replace( second=0, microsecond=0 )

    _nextMnEvent = ((int(_cur_date.minute / settings.AGGREGATION_INTERVAL)+1) * settings.AGGREGATION_INTERVAL)
    if( _nextMnEvent >= 60 ):
        nextEvent = _cur_date.replace(minute=0) + timedelta(hours=1)
    else:
        nextEvent = _cur_date.replace(minute=_nextMnEvent)

    return nextEvent



# #############################################################################
#
# MAIN
#
def main():

    # Global variables
    global ARGS, _shutdownEvent,_condition

    # create threading.event
    _shutdownEvent = threading.Event()

    # Trap CTRL+C (kill -2)
    signal.signal(signal.SIGINT, ctrlc_handler)


    #
    # aggregation interval (i.e downsampling)
    if( 60 < settings.AGGREGATION_INTERVAL < 1 ):
        log.error("INTERVAL must get inside a [1..60] range, aborting")
        sys.exit(1)
    if( (60 % settings.AGGREGATION_INTERVAL) != 0 ):
        log.warning("beware that INTERVAL '%d'mn is not a 60mn multiple ..." % settings.AGGREGATION_INTERVAL )
        time.sleep(5)
    log.info("data aggregation interval is '%d mn.'" % (int(settings.AGGREGATION_INTERVAL)) )


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
    # first, let's initialize things
    #
    # ... unless OVERALL DATABASE PARSER IS ACTIVE
    if( ARGS.init is True ):
        # GLOBAL INITIALIZATION TO PARSE upto ONE YEAR OF DATA
        curTime         = datetime.utcnow()
        agg_start_date  = curTime.replace( year=curTime.year-1, month=curTime.month+1, day=1, hour=0, minute=0, second=0, microsecond=0)

        # defined start_date ?
        if( ARGS.start_date is not None ):
            agg_start_date = datetime.fromisoformat( ARGS.start_date.rstrip('Z') )
            agg_start_date = agg_start_date.replace( second=0, microsecond=0 )

            if( agg_start_date.minute % settings.AGGREGATION_INTERVAL != 0 ):
                # align to next INTERVAL_mn
                agg_start_date = nextAggEvent( agg_start_date )

        # is aggregation start_date > now
        if( agg_start_date > curTime ):
            log.error("defined start_date is in the future ... abort !")
            sys.exit(1)
        #print(start_date)

        # DELETE DATA from sensors_lowres
        # ... mostly because there's an '_avg_count' field that need to get cleared
        dbclient.clearBucket( start=agg_start_date.replace(hour=0,minute=0), stop=datetime.utcnow(),
                                bucket=dbclient.sensors_bucket + '_lowres' )

    else:
        # get latest sensors_hires index and set agg_start_date to next iteration (i.e +1mn)
        last_agg_date  = getAggLastIndex( dbclient )
        if( last_agg_date is None ):
            log.error(f"Failure to start from latest writing '{last_agg_date}' ... you may use --init along with --start-date options")
            time.sleep(4)
            sys.exit(0)     # this way, supervisord will not try to restart us
        if( (datetime.utcnow() - last_agg_date).total_seconds() > 5*60 ):
            agg_start_date = last_agg_date + timedelta( minutes=1, seconds=10 )
        else:
            agg_start_date = None



    #
    # main loop

    # initialise _condition
    _condition = threading.Condition()

    _retry = MAX_RETRIES
    with _condition:

        # start from latest INTERVALmn aggregation
        curTime = agg_start_date

        _lastAggregationMn = None
        while( not _shutdownEvent.is_set() and _retry > 0 ):

            if( agg_start_date is None ):
                curTime = datetime.utcnow().replace(microsecond=0)     # (naive) UTC datetime struct

            # action every INTERVAL mn ...
            if( (curTime.minute % settings.AGGREGATION_INTERVAL)==0 and (curTime.minute!=_lastAggregationMn) ): 

                _agg_date = curTime.replace(second=0,microsecond=0)     # this is stop_date in DATETIME format

                extra_params = dict()
                if( (datetime.utcnow() - _agg_date).total_seconds() > 7*24*60*60 ):
                    # more than one 7 DAYs away from now ==> whole DAY aggregation!
                    extra_params['start_date'] = _agg_date
                    extra_params['end_date'] = _agg_date.replace(hour=23,minute=59,second=59)
            
                print("\n\n\n")
                log.debug(f"start sensors aggregation ... {_agg_date}")
                #time.sleep(0.8)

                try:

                    # extract last interval mn from sensors to our DataFrame
                    # e.g 12:45:01 --> _agg_date=12:50:00
                    data = getSensorsData( dbclient,
                                            aggDate = _agg_date,
                                            # [jun.20] example WIND FILTER
                                            #tags = { 'kind':'wind','unitID':'metropole' },
                                            interval_mn = settings.AGGREGATION_INTERVAL,
                                            **extra_params )

                    if( data is None or
                        ( isinstance(data,list) and len(data)==0 ) or
                        ( isinstance(data,pd.DataFrame) and data.shape[0] == 0 )
                        ):
                        log.debug("probably not enough data ... continuing")
                        #_retry-=1
                    else:
                        # if single table --> set it in a list
                        if( isinstance(data,list) is not True ):
                            # data is not a list ... probably because there's only one DF
                            _input_tables = list()
                            _input_tables.append( data )
                        else:
                            # okay a list of tables
                            _input_tables = data

                        # okay, now let's parse all these tables
                        # Note: there exists several tables because of the various data sensors format
                        for table in _input_tables:

                            if( _shutdownEvent.is_set() ): break

                            # group sensors according to their 'table Idx' (i.e per SENSOR)
                            gb = table.groupby('table', sort=False)
                            for _,group in gb:

                                if( _shutdownEvent.is_set() ): break

                                if( getLogLevel().lower() == 'debug' ): 
                                    print(f">>> nb data rows = {group.shape[0]}")

                                # process group (i.e process measures from a UNIQ sensor)
                                _df = sensor_aggregator( group )
                                if( _df is None or
                                    ( isinstance(_df,pd.DataFrame) and _df.shape[0]==0 ) ):
                                    continue

                                # save aggregated data
                                writeAgg( dbclient, _df, aggDate=_agg_date )
                                pprint_aggregation( _df, _agg_date )    # mainly to see something while running

                                # update DAY in lowres aggregation
                                updateAggDay( dbclient, _df, aggDate=_agg_date )

                        # success
                        _retry = MAX_RETRIES

                except Exception as ex:
                    log.error("Failure either during data aggregation overall process: " + str(ex), exc_info=True )
                    _retry-=1
                    time.sleep(10)
                    continue


                # For lowres aggregation
                # set curTime to the end of the interval :)
                curTime = extra_params.get('end_date',curTime)

                # whatever happened (e.g not enough data), let's continue with next event
                _lastAggregationMn = curTime.minute


            # compute datetime next event
            nextAggregationTime = nextAggEvent( curTime )

            #print(f"computed nextAggregatiomTime: {nextAggregationTime}   (was curTime: {curTime}")

            # note: we'll launch sensors data aggregation 10 seconds after end_date
            # eg. start_date=13:45:01  stop_date=13:50:00  processingTime=13:50:10
            nextAggregationTime = nextAggregationTime.replace( second=10 )

            # then compute timeout seconds to nextAggregation
            _nextTimeout = (nextAggregationTime - datetime.utcnow()).total_seconds()

            if( _nextTimeout <= 0 ):
                # still far away from current time ... immediate aggregation to get launched
                log.debug("still far away from current time ... continuing ...")
                curTime = nextAggregationTime
                continue

            elif( agg_start_date is not None ):
                # _nextTimeout is positive means we're near current time
                #    --> return to regular INTERVALmn aggregation processing
                log.info("WE'RE BACK near current time, switch back to normal operations :)")
                agg_start_date = None
            
            log.info("Next timeout to occur in %d seconds ..." % int(_nextTimeout) )

            # now sleeping till next event
            if( _condition.wait( _nextTimeout ) is False):
                log.debug("timeout reached ...")
            else:
                log.debug("interrupted ... maybe a shutdown ??")
                time.sleep(1)

    # end of main loop
    if( _retry == 0 ):
        log.error("ouch, something went wrong with this app. ... apply for restart ...")
        _shutdownEvent.set()
        time.sleep(7)
        sys.exit(1)

    log.info("app. is shutting down ... have a nice day!")
    _shutdownEvent.set()
    time.sleep(4)

    # delete objects
    del dbclient
    dbclient = None


# Execution or import
if __name__ == "__main__":

    #
    print("\n###\n[neOCampus][dataCOllector] sensors'data downsampling (i.e aggregation) app.\n###")

    # Parse arguments
    parser = argparse.ArgumentParser( allow_abbrev=False,
                         description="neOCampus sensors data aggregator." )

    # RFC-3339 start date
    parser.add_argument( '--start-date', metavar='start_date',
                        dest="start_date", type=str, nargs='?',
                        help="RFC-3339 UTC start_date: data import will start from this date. \
exemple: --start-date=2020-04-09T19:00:00Z" )

    # RFC-3339 end date
    parser.add_argument( '--end-date', metavar='end_date',
                        dest="end_date", type=str, nargs='?',
                        help="RFC-3339 UTC end_date: data import process won't go over this date. \
exemple: --end-date=2020-04-09T19:00:00Z" )

    # aggregation interval
    parser.add_argument( '-i', '--interval', type=int, nargs='?', action='store',
                        dest="interval", metavar="interval", choices=range(1,61),
                        help="Define aggregation interval expressed in minutes." )

    # initializing: compute ALL aggregation for ALL sensors across 1 year (except if start or end date set)
    parser.add_argument( '--init', action="store_true",
                        help="initialize the whole downsampling process to fill in all sensors aggregation data ... huge process!" )

    # simulation mode
    parser.add_argument( '--sim', action="store_true",
                        help="activate simulation mode, i.e no write in database nor mqtt nor ..." )
    # debug mode
    parser.add_argument( '-d', '--debug', action="store_true",
                        help="Enable debug mode (default is False)." )

    ARGS = parser.parse_args()
    #print("\nARGUMENTS:\n" + str(ARGS) + "\n"); sys.exit(0)

    # interval definition
    if( ARGS.interval is not None ):
        if( ARGS.interval > 60 or ARGS.interval < 1 ):
            log.error("out-of-range interval argument [1..60]")
            sys.exit(1)
        settings.AGGREGATION_INTERVAL = ARGS.interval

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

    # Pandas 'settingwithcopywarning' as EXCEPTION in sim / debug mode
    if( getLogLevel().lower() == 'debug' or settings.SIM ):
        print("\n[PANDAS] will raise EXCEPTION on 'settingwithcopywarning'\n")
        pd.set_option('mode.chained_assignment', 'raise')

    # OVERALL INITIALIZATION
    if( ARGS.init is True ):
        log.info("BECAREFULL: you ASKED TO PARSE ALL DATA either from a specific start date or for ONE YEAR !!!")
        log.info("\tthis will DELETE DATA from 'sensors_lowres' bucket ...")
        try:
            _answer = input("\tare you sure [y/N] ?? --> ")
        except KeyboardInterrupt as ex:
            _answer = None
        if( _answer is None or not len(_answer) or _answer.lower()[0] != 'y' ):
            log.info("ABORTING operation ...")
            sys.exit(0)
        else:
            log.info("\n\tOVERALL DATABASE PARSING will start in a few seconds ... CTRL-C to cancel")
            for _ in range(6):
                print('.',end='')
                time.sleep(0.5)

    #sys.exit(0)

    # Start main app.
    main()


# The END - Jim Morrison 1943 - 1971
#sys.exit(0)

