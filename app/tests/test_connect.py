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


#
# Function to handle MQTT messages
def myMsgHandler( topic, payload, *args, **kwargs ):
    ''' function called whenever our MQTT client receive weather data.
        Beware that it's called by mqtt_loop's thread !
    '''

    # check for special topics we're not interested in
    if( topic.startswith('_') or topic.startswith('TestTopic') or "camera" in topic ):
        log.debug("special topic not for us: %s" % str(topic) )
        return

    # extract timestamp ... or set it
    #_dataTime = int(float( payload.get('dateTime', time.time()) ))
    _dataTime = datetime.utcnow()

    log.debug("MSG topic '%s' received ..." % str(topic) )
    print( payload )

    # extract addon parameters ...
    # ... this way to be sure they are defined ;)
    try:
        mydb = kwargs['db']
        valueUnitsIDS = kwargs['valueUnits']
        sensorsIDlist = kwargs['hints']
    except Exception as ex:
        log.error("missing several parameters (!!) : " + str(ex), exc_info=(getLogLevel().lower()=="debug") )
        return
