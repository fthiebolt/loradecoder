#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus LEGACY data processor
#
# These fonctions/class are intended to process data taken from SOURCE database
#   to get ingest to TARGET database.
# This one is specific to neOCampus legacy data (i.e Hamdi's data)
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
from datetime import datetime, timezone, timedelta
import threading

# mongodb
from bson.objectid import ObjectId

# Logging
import logging

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

# base data processing
from .base_processor import baseProcessor

# settings
import settings



# #############################################################################
#
# Global variables
# (scope: this file)
#




# #############################################################################
#
# Functions
#



# #############################################################################
#
# Class
#
class legacyProcessor(baseProcessor):
    ''' neOCampus LEGACY data import processor class:
        It is intended to grab data from a SRC database (legacy one) 
        and to write processed data to a DST database
    '''

    #
    # class attributes ( __class__.<attr_name> )
    # Batch processing limits: we define a maximum BATCH_PROCESSING_LIMIT (i.e maximum number of) data to process
    #   and a maximum BATCH_PROCESSING_INTERVAL number of seconds of input data to process
    # Hence, process epoch will end whenever either of these boundaries are reached
    BATCH_PROCESSING_LIMIT      = 64000     # number of messages to process on each processor iteration
                                            # [apr.20] it seems to exist a limit around 65536
    BATCH_PROCESSING_INTERVAL   = 60*60*24  # number of seconds of data input to process for each iteration


    #
    # objects attributes
    sim                 = False

    _src                = None  # SOURCE database client
    _dst                = None  # TARGET database client
    _collection_name    = None  # name of the collection to import (e.g 'SGE')
    _start_date         = None  # optional start date for data import
    _end_date           = None  # optional end date for data import
    _start_id           = None  # optional start-id for data import
    _disable_check      = False # global option not take to check while inserting data

    _addons             = None  # additional parameters

    overall_processed   = 0     # total number of messages processed


    #
    # object initialization
    def __init__( self, src, dst, collection_name, *args, **kwargs ):
        super().__init__(*args, **kwargs)

        log.debug("initializing data importer module")

        self._src               = src
        self._dst               = dst
        self._collection_name   = collection_name
        self._addons            = kwargs

        # check for _shutdown event
        self._shutdownEvent = self._addons.get('_shutdownEvent')
        if( self._shutdownEvent is None ):
            log.warning("unspecified global shutdown ... thus locally specified ...")
            self._shutdownEvent = Event()

        # check for simulator mode (i.e read-only mode)
        self.sim = self._addons.get('sim',False)
        if( self.sim is True ):
            log.info("[SIM] read-only mode ACTIVATED ... means NO PUBLISH AT ALL!")

        # check for additional parameter: start_date
        # Note: argument is a str; datetime import does not support traling 'Z' like rfc3339
        if( self._addons.get('start_date') is not None ):
            _my_date = self._addons.get('start_date')
            if( _my_date[-1].lower() == 'z' ): _my_date = _my_date[:-1]
            self._start_date = datetime.fromisoformat(_my_date)
            #self._start_date = datetime.replace(tzinfo=timezone.utc)   we prefer naive utc

        # check for additional parameter: end_date
        # Note: argument is a str; datetime import does not support traling 'Z' like rfc3339
        if( self._addons.get('end_date') is not None ):
            _my_date = self._addons.get('end_date')
            if( _my_date[-1].lower() == 'z' ): _my_date = _my_date[:-1]
            self._end_date = datetime.fromisoformat(_my_date)
            #self._end_date = datetime.replace(tzinfo=timezone.utc)   we prefer naive utc

        # overall disable duplicate check flag
        self._disable_check = self._addons.get('disableDuplicateCheck',False)

        # check for additional parameter: start_id
        self._start_id      = self._addons.get('start_id')

        # display status
        self.status()

        log.debug("initialization done")


    def status( self ):
        ''' status '''
        log.info("=== dataImpoter module ===" )
        log.info("=== SOURCE database: %s" % self._src )
        log.info("===    \---> collection: %s" % self._collection_name )
        if( self._start_date is not None ):
            log.info("===    |--> start: %s" % str(self._start_date) )
        if( self._start_id is not None ):
            log.info("===    |--> start_id: %s" % self._start_id )
        log.info("=== TARGET database: %s" % self._dst )


    ''' load method, called by super class before starting processor '''
    def load( self ):
        log.debug("[LOAD] legacy_processor is preparing ...")

        # extract id of sensors from mongoDB
        sensorID = dict()
        _iter = self._src.find( 'typecapteur' )
        # parse 'typecapteur' collection (e.g temperature, co2, humidity etc etc)
        for each in _iter:
            # parse sensors: each sensor has an ID
            # key nomCapteur: <topic/unitID/subID> e.g u4/campusfab/temperature/auto_92F8/79
            #   value = list( id associated with <topic/unitID/subID>, id piece )
            for inside in each["Capteurs"]:
                sensorID[ inside["idCapteur"] ] = inside["nomCapteur"]

        print("sensorID: " + str(sensorID) )

        # mongodb filter
        _mongo_filter = dict()
        # | start_id
        if( self._start_id is not None ):
            try:
                _mongo_filter['_id'] = { "$gte" : ObjectId(self._start_id) }
            except Exception as ex:
                log.warning("while creating mongoDB filter from _id: " + str(ex) )
                self._shutdownEvent.set()
                sys.exit(1)
        # | start_date
        elif( self._start_date is not None ):
            try:
                # generate an id from data
                # [apr.20] there exist a two hours shift between ObjectId time and real utc measuretime ?!?!
                _mongo_filter['_id'] = { "$gte" : ObjectId.from_datetime(self._start_date) }
            except Exception as ex:
                log.warning("while creating mongoDB filter from _id: " + str(ex) )
                self._shutdownEvent.set()
                sys.exit(1)

        # generate mongoDB iterator
        _src_iterator = self._src.find( self._collection_name, query=_mongo_filter )
        #_src_iterator = self._src.find( self._collection_name, query=_mongo_filter, skip=1000000 )

        # generate importer iterator
        cur_iter = dict()
        cur_iter['sensorID'] = sensorID
        cur_iter['iterators'] = [ _src_iterator ]

        self.overall_processed = 0

        #log.info("MongoDB connection is UP featuring:\n\t{0:,d} measures :)\n\t{1:,d} unmanaged measures :(".format(mydb.measure.count(),mydb.failedData.count()) )

        return cur_iter


    ''' processor method called by super class to process incoming data
[apr.20] neocampus database, measure collection, skip=1000000
{'_id': ObjectId('5972792fadf801c231cd1322'), 'idcapteur': 10, 'subId': 'ilot3', 'device': 'luminosity', 'data': {'date': '2017-07-21T21:59:11.841071', 'payload': {'subID': 'ilot3', 'input': 110, 'value_units': 'lux', 'value': 0, 'unitID': 'inside'}, 'uri': 'u4/302/luminosity/ilot3'}, 'mesurevaleur': [{'idlibv': 3, 'valeur': 0.0}], 'building': 'u4', 'room': '302', 'idpiece': 1, 'uri': 'u4/302/luminosity/ilot3', 'idMesure': 1000099, 'datemesure': datetime.datetime(2017, 7, 21, 19, 59, 11, 841000)}
Time: '2017-07-21 21:59:11+00:00'  URI: u4/302/luminosity/inside/ilot3'  value: '0.0'
    '''
    def processor( self, cur_iter ):
        ''' processing will stop once this method returns None '''
        log.debug("[PROCESSOR] is starting ...")

        failedIDs = list()
        _nb_processed = 0
        _batch_end_time = None
        _firstTime = True
        _disableTargetInsertionCheck = False
        for _measure in cur_iter['iterators'][0]:
            try:

                # 1: DATA TIME
                _measureTime = None
                if( 'datemesure' in _measure.keys() ):
                    _measureTime = _measure['datemesure']
                elif( 'date' in _measure.keys() ):
                    _measureTime = _measure['date']
                else:
                    raise Exception("no time of measure found ?!?!")
                
                #print( _measure )
                #_dataTime = _measure['_id'].generation_time  [apr.20] not UTC ?!?!
                #_uri = cur_iter['sensorID'][ _measure['idcapteur'] ]
                #_value = _measure['mesurevaleur'][0]['valeur']
                #if( str(_value).lower() == "nan" ): continue
                
                if( isinstance(_measureTime, str) ):
                    _dataTime = datetime.fromisoformat( _measureTime )
                elif( isinstance(_measureTime, datetime) ):
                    _dataTime = _measureTime
                else:
                    raise Exception("unsupported 'datemesure' format !")


                # 2: TOPIC
                if( 'topic' in _measure.keys() ):
                    _uri = _measure['topic']
                elif( 'uri' in _measure.keys() ):
                    _uri = _measure['uri']
                else:
                    # extract from sensorIDs
                    _uri = cur_iter['sensorID'][ _measure['idcapteur'] ]

                _topicTokens = _uri.split('/')[:3]

                topic = "/".join(_topicTokens)


                # FAILEDDATA collection filter
                #if( 'energy' in topic ): continue
                #if( 'digital' in topic ): continue
                #if( topic.startswith('bu') is not True or topic.endswith('temperature') is not True ): continue

                #if( 'shutter' not in topic ): continue


                # 3: PAYLOAD
                if( 'payload' in _measure.keys() ):
                    payload = _measure['payload']
                elif( 'data' in _measure.keys() ):
                    payload = _measure['data']['payload']
                elif( 'mqtt_data' in _measure.keys() ):
                    payload = _measure['mqtt_data']['payload']
                else:
                    # further processing needed
                    raise Exception("no payload found ?!?!")

                if( isinstance( payload, str ) ):
                    # import json dump
                    try:
                        payload = json.loads(payload.decode('utf-8'))
                    except Exception as ex:
                        payload = json.loads(payload)


                # FAILEDDATA collection filter to skip SHUTTER with wrong 'value' field (2017)
                #if( payload.get('value') == 'value' ): continue
                #if( payload.get('unitID') != "metropole" ): continue


                print("\n_id: '%s'  Time: '%s'  topic: %s'  payload: '%s'" % (str(_measure['_id']),_dataTime,topic,payload) )

                # TARGET database data insertion ... same as MQTT message except we add 'timestamp'
                # HOWEVER, legacy data are coming from MQTT, hence timestamp has been applied by receiver
                # ... and may differ (a little little bit) between them ==> we need to check whether it doesn't
                # already exist

                # check if data already exists within the target DB
                if( _firstTime is True ):
                    _firstTime = False

                    if( self._disable_check is False ):
                        # get stream
                        _qstream = self._dst.find( _dataTime,
                                                    interval=__class__.BATCH_PROCESSING_INTERVAL,
                                                    fields=['value'] )  # we don't care about data !
                        try:
                            _rec = next(_qstream)
                            if( _rec is not None ):
                                # data already exists, we need to check for duplicate
                                _disableTargetInsertionCheck = False
                            else:
                                # means there does not exist any data within this range --> no duplicate checking :)
                                log.info("[SPEED UP] interval '%s' --> + %d seconds is not within target DB :)" % (_dataTime,__class__.BATCH_PROCESSING_INTERVAL))
                                _disableTargetInsertionCheck = True
                        except Exception as ex:
                            # means there does not exist any data within this range --> no duplicate checking :)
                            log.info("[SPEED UP] interval '%s' --> + %d seconds is not within target DB :)" % (_dataTime,__class__.BATCH_PROCESSING_INTERVAL))
                            _disableTargetInsertionCheck = True
                    else:
                        # global disable check for duplicate
                        log.info("[SPEED UP] OVERALL disable duplicate check flag activated ...")
                        _disableTargetInsertionCheck = True

                    # compute batch_end_time threshold
                    _batch_end_time = _dataTime + timedelta(seconds=abs(int(__class__.BATCH_PROCESSING_INTERVAL)))
                    if( self._end_date is not None and _batch_end_time > self._end_date ):
                        log.debug("batch_end_time set to upper limit %s" % self._end_date )
                        _batch_end_time = self._end_date


                # launch data insertion to target DB
                params = dict()
                params['timestamp'] = _dataTime
                if( _disableTargetInsertionCheck is True ):
                    params['forceNoDuplicateCheck'] = True
                _res = self._dst.msgHandler( topic, payload, **params )

                if( self.sim is not True ):
                    if( _res is True ):
                        log.info(f"[OK] --> nb_processed = {_nb_processed:,d}")
                    else:
                        log.info("...duplicate...")

            except (SyntaxError,ValueError,NameError,AttributeError) as ex:
                log.error("hum hum ... :( " + str(ex) )
                return None
            except Exception as ex:
                log.error ("something went wrong while processing:\n%s\n" % (str(_measure)) + str(ex) )
                failedIDs.append( _measure['_id'] )
                time.sleep(1.2)
                #break  # [DEBUG]: stop on first exception
                continue

            #if( 'shutter' in topic ):
            #    self._shutdownEvent.set()
            #    sys.exit(1)

            _nb_processed +=1
            #print(f"\tcurrently processed messages = {_nb_processed:,d}"); time.sleep(0.5)
            if( _nb_processed >= __class__.BATCH_PROCESSING_LIMIT ): break
            if( _dataTime >= _batch_end_time ): break
            if( self._shutdownEvent.is_set() ): break

            #break  # [DEBUG]: stop on first successful insertion


        # failed data
        if( len(failedIDs) ):
            # save failed data for further (manual) processing
            log.info("at least %d messages need further processing" % len(failedIDs) )
            #TODO: save these _id to a file
            print(failedIDs)

        # update overall processed messages
        self.overall_processed += _nb_processed

        # display synthesis
        print(f"\nCurrent Overall Processed messages: {self.overall_processed:,d}" )
        if( _dataTime is not None ):
            print(f"Timestamp from last processed message: {_dataTime}" )

        # shutdown is on way ?
        if( self._shutdownEvent.is_set() ): return None

        if( self._end_date is not None and _dataTime >= self._end_date ):
            #processing is over
            log.info("[SUCCESS] reached end of data processing range --> '%s' :)" % str(self._end_date) )
            return None
        elif( _nb_processed < __class__.BATCH_PROCESSING_LIMIT and
                _dataTime < _batch_end_time ):
            # probably the end of the current iterator
            if( len(cur_iter['iterators']) == 1 ):
                return None

            # switch to next iterator
            log.info("\t>>> SWITCHING TO NEXT ITERATOR <<<")
            cur_iter['iterators'] = cur_iter['iterators'][1:]

        #time.sleep(2)   # why not ;)

        #return None   # [DEBUG] for stop after first batch
        return cur_iter


    ''' quit method, called by super class after having ran the data processor '''
    def quit( self ):
        log.debug("[QUIT] end of legacy_processor ...")
        #log.info("Total number of messages processed: " + str(self.overall_processed) )
        log.info(f"Total number of messages processed: {self.overall_processed:,d}" )

