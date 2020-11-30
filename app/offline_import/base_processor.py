#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus generic data import processor
#
# These fonctions/class are intended to import data from a src database
#   to a target database
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
from threading import Thread, Event

# Logging
import logging

# --- project imports
# logging facility
from logger.logger import log, setLogLevel, getLogLevel

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
class baseProcessor(Thread):
    ''' Generic data importing processor class:
        It is intended to grab data from a SRC database and to write
        them to a DST database
    '''

    #
    # class attributes ( __class__.<attr_name> )

    #
    # objects attributes
    sim                 = False # read-only mode (i.e no publish)

    _addons             = None  # additional parameters


    #
    # object initialization
    def __init__( self, *args, **kwargs ):
        super().__init__()

        self._addons            = kwargs

        # check for _shutdown event
        self._shutdownEvent = self._addons.get('_shutdownEvent')
        if( self._shutdownEvent is None ):
            log.warning("unspecified global shutdown ... thus locally specified ...")
            self._shutdownEvent = Event()

        # check for simulator mode (i.e read-only mode)
        self.sim = self._addons.get('sim',False)


    #
    # called by Threading.start()
    def run( self ):

        # load module
        log.info("module loading")
        cur_iter = self.load()

        # start processing
        log.info("start import processor ...")

        # launch
        try:
            while not self._shutdownEvent.is_set():

                #
                #
                # DATA PROCESSING
                #
                #
                cur_iter = self.processor( cur_iter )
                if( cur_iter is None ):
                    # end of data processing
                    break

                # TODO: to remove
                time.sleep(2)

            log.debug("end of processing ...")

        except Exception as ex:
            if getLogLevel().lower() == "debug":
                log.error("module crashed (high details): " + str(ex), exc_info=True)
            else:
                log.error("module crashed: " + str(ex))
            time.sleep(1)

        # shutdown module
        log.info("module stopping")
        self.quit()

        # end of thread
        log.info("Thread end ...")


    ''' status method, to be implemented by subclasses '''
    def status( self ):
        pass

    ''' load method, to initialize modules before running, to be implemented by subclasses '''
    def load(self):
        pass

    ''' process method, data processing, to be implemented by subclasses '''
    def processor(self):
        pass

    ''' quit method, to shutdown modules before exiting, to be implemented by subclasses '''
    def quit(self):
        pass

