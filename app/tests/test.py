#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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

from test2 import TestModule

# settings
import testSettings


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
    # Test2Module
    print("Instantiate Test2Module ...")

    myModule = TestModule()

    # test SIM mode within mqtt client
    while( not _shutdownEvent.is_set() ):
        time.sleep(1)

    # end of main loop
    log.info("app. is shutting down ... have a nice day!")
    _shutdownEvent.set()
    time.sleep(4)



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
        testSettings.SIM = True
        time.sleep(1)

    #sys.exit(0)

    # Start main app.
    main()


# The END - Jim Morrison 1943 - 1971
#sys.exit(0)

