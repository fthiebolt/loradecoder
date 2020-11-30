#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus MongoDB management module for:
#   - neOCampus data @ MongoDB (i.e legacy data ---Hamdi's one)
#   - datalake @ MongoDB (i.e new landing area started mid-2020)
#
# [Apr.20] F.Thiebolt   initial release
#



# #############################################################################
#
# Import zone
#
import os
import sys
import time
import json
from datetime import datetime,timezone
from threading import Event
from random import randint

# MongoDB client
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus

# --- project related imports
from logger.logger import log, getLogLevel
import settings



# #############################################################################
#
# Class
#
class MongoModule(object):
    ''' MongoDB facility module:
        handle everything related to the MongoDB database
    '''


    #
    # class attributes ( __class__.<attr_name> )
    DEFAULT_LOCATION    = 'ut3'     # when no information is provided about site location

    #
    # objects attributes
    sim                 = False # read-only mode (i.e no publish)

    _client             = None  # mongo database client
                                # i.e client connected to mongoDB and tied to a specific database

    _database           = None
    _server             = None
    _port               = None
    _user               = None
    _passwd             = None
    _collection_name    = None  # name of default collection (i.e ops about an unspecified collection)

    _addons             = None  # additional parameters
    

    #
    # object initialization
    def __init__( self, user, passwd, server, port, database, *args, **kwargs ):

        log.debug("initializing mongoDB module")

        self._user          = user
        self._passwd        = passwd
        self._server        = server
        self._port          = int(port)
        self._database      = database
        self._addons        = kwargs

        # check for _shutdown event
        self._shutdownEvent = self._addons.get('_shutdownEvent')
        if( self._shutdownEvent is None ):
            log.warning("unspecified global shutdown ... thus locally specified ...")
            self._shutdownEvent = Event()

        # check for simulator mode (i.e read-only mode)
        self.sim = self._addons.get('sim',False)
        if( self.sim is True ):
            log.info("[SIM] read-only mode ACTIVATED ...")

        # check for a default collection
        self._collection_name =  self._addons.get('collection_name')

        # setup database connection
        self._connect()
            
        # display status
        self.status()

        log.debug("initialization done")


    def __repr__( self ):
        ret = "\n"
        ret += "=== MongoDB connexion ===" + '\n'
        ret += "=== MongoDB server: %s:%d" % (self._server,self._port) + '\n'
        ret += "=== MongoDB database: %s" % self._database + '\n'
        return ret


    def is_connected( self ):
        ''' are we connected ? '''
        return True if self._client is not None else False


    ''' send back status '''
    def status( self ):
        log.info("=== MongoDB connexion ===")
        log.info("=== MongoDB server: %s:%d" % (self._server,self._port) )
        log.info("=== MongoDB database: %s" % self._database)
        if( self._collection_name is not None ):
            log.info("=== MongoDB default collection: %s" % self._collection_name)


    def msgHandler( self, topic, payload, *args, **kwargs ):
        ''' function called whenever our MQTT client receive sensors data.
            Beware that it's called by mqtt_loop's thread !
            return True is data has been properly written, False if error/exception, None otherwise
        '''

        # check for special topics we're not interested in
        if( topic.startswith('_') or topic.startswith('TestTopic') or "camera" in topic
            or "access" in topic or topic.endswith('command') or 'value' not in payload.keys() ):
            log.debug("[NOT A SENSOR] either payload WITHOUT 'value' or special topic not for us: %s" % str(topic) )
            return None

        #log.debug("MSG topic '%s' received ..." % str(topic) )
        #if getLogLevel().lower() == "debug":
        #    print( payload )


        mongo_data = dict()

        # 1: MEASURETIME

        # timestamp could be:
        # - argument to this function (expected datetime structure)
        # - within payload
        # - locally generated
        # Note: if timestamp is locally generated ==> a priori no possible data duplicate
        #   ... otherwise we need to check ...
        _checkDataInsertion = True
        _dataTime = kwargs.get('timestamp')
        if( _dataTime is None ):
            for _key in settings.MQTT_PAYLOAD_TIMESTAMPS:
                if( _key in payload.keys() ):
                    _dataTime = payload.get(_key)
                    break

        if( _dataTime is None ):
            _checkDataInsertion = False
            _dataTime = datetime.now(timezone.utc)
        elif( kwargs.get('forceNoDuplicateCheck') is True ):
            log.debug("force flag not to check for duplicate activated ...")
            _checkDataInsertion = False

        # [apr.20] lowering precision to seconde
        mongo_data['measuretime'] = _dataTime.replace(microsecond=0)


        # 2: TOPIC
        mongo_data['topic'] = topic


        # 3: PAYLOAD
        mongo_data['payload'] = payload


        #if( getLogLevel().lower() == "debug" or self.sim is True ):
        #    print( mongo_data )

        # check for duplicate ?
        if( _checkDataInsertion is True ):
            raise Exception("not yet implemented")
            #if( self._is_duplicate( mongo_data ) is True ):
            #    log.debug("DATA is duplicate ... cancel insertion")
            #    return None

        # SIMULATION mode ?
        if( self.sim ):
            log.info("[SIM] read-only mode active: no database writings")
            return None

        # now write data to collection
        try:
            _id = self._client[ self._collection_name ].insert_one( mongo_data )

        except Exception as ex:
            log.warning("exception detected while inserting measure: " + str(ex) )

        if getLogLevel().lower() == "debug":
            print(f"[{__class__.__name__}] _id: '{_id.inserted_id}'  Time: '{mongo_data['measuretime']}'  topic: '{mongo_data['topic']}'  payload: '{mongo_data['payload']}'")
            #print("\n_id: '{%s}'  Time: '%s'  topic: '%s'  payload: '%s'" % (str(_id.inserted_id),mongo_data['measuretime'],mongo_data['topic'],mongo_data['payload']) )

        # tada ! :)
        return True


    def find( self, collection=None, query=None, skip=None ):
        ''' return iterator related to a collection with an optional query '''
        try:
            params = dict()
            if( query is not None ): params['filter'] = query
            if( skip is not None ): params['skip'] = int(skip)
            if( collection is None ): collection = self._collection_name
            return self._client[ collection ].find( **params )
        except Exception as ex:
            log.error("exception while accessing collection '%s' with query '%s': " % (str(collection),str(query)) + str(ex) )
            return None


    # -----------------------------------------------------------
    # low-level methods
    # - _connect()      connect to database
    #

    def _connect( self ):
        ''' get connected to the database '''

        if( self._client is not None ):
            log.warning("_client is Not None ?!?! ... continuing ...")
            time.sleep(2)
        try:
            url = "mongodb://%s:%s@%s:%d" % ( quote_plus(self._user), quote_plus(self._passwd), self._server, self._port)
            log.debug("MongoDB URL : " + str(url))
            client = MongoClient( url, connect=True )
            self._client = client[ self._database ]
        except Exception as ex:
            log.error("unable to connect to database '%s': " % (url) + str(ex))
            self._client = None


