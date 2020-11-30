#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#



# #############################################################################
#
# Import zone
#
import os
import sys
import time
import json

# --- project related imports
import testSettings



# #############################################################################
#
# Class
#
class TestModule():

    # class attributes ( __class__.<attr_name> )

    #
    # object initialization
    def __init__(self, *args, **kwargs ):

        if( testSettings.SIM ):
            print("[SIM] mode activated ...")
        else:
            print("normal mode (i.e no SIM modes)")

