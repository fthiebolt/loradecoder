#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# loradecoder app.
#
# This app. decodes raw LoRa messages into new messages repulished in our MQTT
#   broker in a way to get interpreted by our dataCOllector.
#
# F.Thiebolt    nov.20  initial release
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

# URL parser
from urllib.parse import quote_plus

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

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






# TO BE CONTINUED:
# - remove everything from MongoDB
# - implement extended Cayenne LPP decoder
# - ...





# #
# # Function to get connected to the MongoDB database
# def connect_db( user, password, server, port, database ):
#     db = None
#     try:
#         uri = "mongodb://%s:%s@%s:%d" % ( quote_plus(user), quote_plus(password), server, port)
#         log.debug("MongoDB URI : " + str(uri))
#         client = MongoClient( uri )
#         db = client[ database ]
#     except Exception as ex:
#         log.error("unable to connect to MongoDB: " +str(ex))
#         db = None
#     return db


#
# Decorator to MshHandler function
# def _myMsgHandler( func ):
#     ''' Handle MQTT messages with additional stuff like
#         db, valueUnits and hints as parameters
#     '''
#     def _wrapper( *args, **kwargs ):
#         kwargs['db'] = mydb
#         kwargs['valueUnits'] = valueUnits
#         kwargs['hints'] = hints

#         # call to function
#         func( *args, **kwargs )

#     return _wrapper


#
# Function to handle MQTT messages
# @_myMsgHandler 
# def myMsgHandler( topic, payload, *args, **kwargs ):
#     ''' function called whenever our MQTT client receive weather data.
#         Beware that it's called by mqtt_loop's thread !
#     '''

#     # check for special topics we're not interested in
#     if( topic.startswith('_') or topic.startswith('TestTopic') or "camera" in topic
#          or "access" in topic or topic.endswith('command') or 'value' not in payload.keys() ):
#         log.debug("[NOT A SENSOR] either payload WITHOUT 'value' or special topic not for us: %s" % str(topic) )
#         return

#     # extract timestamp ... or set it
#     #_dataTime = int(float( payload.get('dateTime', time.time()) ))
#     _dataTime = datetime.utcnow()

#     log.debug("MSG topic '%s' received ..." % str(topic) )
#     print( payload )

#     # extract addon parameters ...
#     # ... this way to be sure they are defined ;)
#     try:
#         mydb = kwargs['db']
#         valueUnitsIDS = kwargs['valueUnits']
#         sensorsIDlist = kwargs['hints']
#     except Exception as ex:
#         log.error("missing several parameters (!!) : " + str(ex), exc_info=(getLogLevel().lower()=="debug") )
#         return

#     # compute key: <topic/unitID/subID>
#     _keyTokens = [ topic, payload.get('unitID'), payload.get('subID') ]
#     _keyTokens = [ str(token) for token in _keyTokens if token ]
#     sensorID = "/".join( _keyTokens )

#     # split tokens from topic
#     items = topic.split("/")

#     # future id of message
#     # TBC: do we *REALLY* need to do this ??
#     # deprecation warning !
#     idm = mydb.measure.count()

#     # SIMULATION mode ?
#     if( settings.SIM ):
#         log.debug("[SIM] read-only mode active: no mod applied")
#         return

#     try:
#         _uri = topic+"/"+str(payload.get("subID"))

#         if( sensorID in sensorsIDlist):
#             # KNOWN SENSOR

#             _idvalUnit = valueUnitsIDS.get( payload.get("value_units") )

#             if( _idvalUnit is None ):
#                 raise Exception("unknown 'value_units':%s from known sensor '%s' ?!?!" % (str(payload.get("value_units")),sensorID))

#             # insert
#             # Note: strange uri ?!?!
#             mydb.measure.insert_one( { "building" : items[0] ,"room" : items[1] ,"device" : items[2],
#                                     "subId" : payload.get("subID"), "uri" : _uri,
#                                     "datemesure": _dataTime, "idMesure" : idm,
#                                     "idcapteur" : sensorsIDlist[sensorID][0], "idpiece" : sensorsIDlist[sensorID][1],
#                                     "mesurevaleur" : [ { "idlibv" : _idvalUnit, "valeur" : float(payload.get("value")) } ],
#                                     "data" : { "payload" : payload, "date" : _dataTime.isoformat() , "uri" : _uri }
#                                  } )
#             log.debug("[%s] known sensor inserted :)" % sensorID )

#         else:
#             # UNKNOWN SENSOR
#             mydb.measure.insert_one( { "building" : items[0] ,"room" : items[1] ,"device" : items[2],
#                                     "subId" : payload.get("subID"), "uri" : _uri,
#                                     "datemesure": _dataTime, "idMesure" : idm,
#                                     "data" : { "payload" : payload, "date" : _dataTime.isoformat() , "uri" : _uri }
#                                  } )

#             log.debug("[%s] UNKNOWN sensor inserted" % sensorID )

#     except Exception as ex:
#         log.warning("exception detected while inserting measure: " + str(ex) )
#         log.info("[exception][%s] add measure to failedData collection for further processing" % sensorID )
#         mydb.failedData.insert_one({'topic': topic , 'date': _dataTime, 'payload': payload})



# #############################################################################
#
# MAIN
#
def main():

    # Global variables
    global _shutdownEvent, _condition, mydb, valueUnits, hints

    # create threading.event
    _shutdownEvent = threading.Event()

    # Trap CTRL+C (kill -2)
    signal.signal(signal.SIGINT, ctrlc_handler)


    # #
    # # MongoDB
    # log.info("Initiate connection to MongoDB.neocampus database ...")

    # _mongo_user = os.getenv("MONGO_USER")
    # if( _mongo_user is None or _mongo_user == "" ):
    #     log.error("unspecified MONGO_USER ... aborting")
    #     time.sleep(3)
    #     sys.exit(1)
    
    # _mongo_passwd = os.getenv("MONGO_PASSWD")
    # if( _mongo_passwd is None or _mongo_passwd == "" ):
    #     log.error("unspecified MONGO_PASSWD ... aborting")
    #     time.sleep(3)
    #     sys.exit(1)

    # _mongo_server = os.getenv("MONGO_SERVER", settings.MONGO_SERVER)
    # if( _mongo_server is None or _mongo_server == "" ):
    #     log.error("unspecified MONGO_SERVER ... aborting")
    #     time.sleep(3)
    #     sys.exit(1)

    # _mongo_port = os.getenv("MONGO_PORT", settings.MONGO_PORT)
    # if( _mongo_port is None or _mongo_port == "" ):
    #     log.error("unspecified MONGO_PORT ... aborting")
    #     time.sleep(3)
    #     sys.exit(1)

    # _mongo_database = os.getenv("MONGO_DATABASE", settings.MONGO_DATABASE)
    # if( _mongo_database is None or _mongo_database == "" ):
    #     log.error("unspecified MONGO_DATABASE ... aborting")
    #     time.sleep(3)
    #     sys.exit(1)

    # # connect ...
    # mydb = connect_db( _mongo_user,
    #                     _mongo_passwd,
    #                     _mongo_server,
    #                     _mongo_port,
    #                     _mongo_database )

    # if( mydb is None ):
    #     log.error("unable to connect to MongoDB ?!?! ... aborting :(")
    #     time.sleep(3)
    #     sys.exit(1)

    # # extract things from mongoDB
    # valueUnits  = dict()
    # hints = dict()
    # # parse 'typecapteur' collection (e.g temperature, co2, humidity etc etc)
    # for each in mydb.typecapteur.find():
    #     # parse units of values (e.g luminosity --> lux (inside), w/m2 (outside)
    #     for inner in each["Libvals"] :
    #         valueUnits[inner["unite"]] =  inner["idLibVal"]

    #     # parse sensors: each sensor has an ID
    #     # key nomCapteur: <topic/unitID/subID> e.g u4/campusfab/temperature/auto_92F8/79
    #     #   value = list( id associated with <topic/unitID/subID>, id piece )
    #     for inside in each["Capteurs"]:
    #         hints[ inside["nomCapteur"] ] = [ inside["idCapteur"], inside["Piece_courante"]["idPiece"] ]

    # print("valueUnits : " + str(valueUnits) )
    # print("hints : " + str(hints) )

    # log.info("MongoDB connection is UP featuring:\n\t{0:,d} measures :)\n\t{1:,d} unmanaged measures :(".format(mydb.measure.count(),mydb.failedData.count()) )
    # time.sleep(2)


    #
    # MQTT
    log.info("Instantiate MQTT communications module ...")

    params = dict()
    
    # shutown master event
    params['_shutdownEvent'] = _shutdownEvent

    # credentials
    _mqtt_user = os.getenv("MQTT_USER", settings.MQTT_USER)
    if( _mqtt_user is None or not len(_mqtt_user) ):
        log.error("unspecified MQTT_USER ... aborting")
        sys.exit(1)
    params['mqtt_user'] = _mqtt_user

    _mqtt_passwd = os.getenv("MQTT_PASSWD", settings.MQTT_PASSWD)
    if( _mqtt_passwd is None or not len(_mqtt_passwd) ):
        log.error("unspecified MQTT_PASSWD ... aborting")
        sys.exit(1)
    params['mqtt_passwd'] = _mqtt_passwd

    # topics to subscribe and addons
    try:
        _mqtt_topics = json.loads(os.getenv("MQTT_TOPICS"))
    except Exception as ex:
        # failed to fing env var MQTT_TOPICS ... load from settings
        _mqtt_topics = settings.MQTT_TOPICS
    if( _mqtt_topics is None or not len(_mqtt_topics) ):
        log.error("unspecified or empty MQTT_TOPICS ... aborting")
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
        log.error("coucou j'ai ressus un message")
        # register own message handler
        # client.handle_message = myMsgHandler

        # ... then start client :)
        client.start()

    except Exception as ex:
        if getLogLevel().lower() == "debug":
            log.error("unable to start MQTT comm module (high details): " + str(ex), exc_info=True)
        else:
            log.error("unable to start MQTT comm module: " + str(ex))
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



# Execution or import
if __name__ == "__main__":

    #
    print("\n###\nneOCampus legacy dataCOllector app.\n###")

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

