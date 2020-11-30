#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus logging facility
#
# Notes:
#
# F.Thiebolt    Jan.20  initial release
#



# #############################################################################
#
# Import zone
#
import logging

# project's related import
from settings import LOG_LEVEL



# #############################################################################
#
# Global variables
#

# Setup logging
logging.raiseExceptions = False
_log_format = logging.Formatter('[%(asctime)s][%(module)s:%(funcName)s:%(lineno)d][%(levelname)s] %(message)s')
_stream_handler = logging.StreamHandler()

log = logging.getLogger()

log.setLevel(LOG_LEVEL)
_stream_handler.setLevel(LOG_LEVEL)

_stream_handler.setFormatter(_log_format)
log.addHandler(_stream_handler)



# #############################################################################
#
# Functions
#

'''
#
# Function to initialize python logging
def init_remote_logger(login, password):
    log.debug("setting up remote logger")
    _url='/device/logger'
    _remote_handler = logging.handlers.HTTPHandler(host=SENSO_HOST, url=_url, method='POST',
                                                   secure=True, credentials=(login, password))
    # remote handler ONLY send ERROR to sensOCampus logger
    _remote_handler.setLevel(logging.ERROR)
    log.addHandler(_remote_handler)
    log.info("Started remote logging to host=https://%s%s ... " % (SENSO_HOST,_url) )
'''

#
# Function to change logger's log level
def setLogLevel( logLevel ):
    if( logLevel is None or log.getEffectiveLevel() == logLevel ):
        log.debug("same log level as the current one ... thus nothing to change ;)")
        return
    log.debug("changing logger level to " + str(logLevel))
    try:
        log.setLevel(logLevel)
        _stream_handler.setLevel(logLevel)
    except ValueError as err:
        log.error("Exception while setting logger to logLevel %s !" % (str(logLevel)) )
        return False
    return True


#
# Function to retrieve current logger's log level
def getLogLevel():
    return logging.getLevelName(log.getEffectiveLevel())

