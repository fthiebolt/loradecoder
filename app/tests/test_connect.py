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
# from logger.logger import log, setLogLevel, getLogLevel

# MQTT facility
# from mqttConnect import CommModule

# settings
# import settings

import paho.mqtt.client as paho

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
    print("<CTRL + C> action detected ...")
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




# #####
# # MQTT
# print("Instantiate MQTT communications module ...")
# params = dict()

# # shutown master event
# params['_shutdownEvent'] = _shutdownEvent
# params['mqtt_user'] ="test"
# params['mqtt_passwd'] ="test"
# params['mqtt_topics'] ="TestTopic/lora/#"
# params['mqtt_server'] ="neocampus.univ-tlse3.fr"
# params['mqtt_port'] =1883
# params['unitID'] =None


# client = None

# try:
#     # init client ...
#     client = CommModule( **params )
#     # ... then start client :)
#     client.start()

# except Exception as ex:
#     print("unable to start MQTT comm module (high details): " )
#     sys.exit(1)

broker="neocampus.univ-tlse3.fr"
port=1883
#define callback
def on_message(client, userdata, message):
    time.sleep(1)
    print("received message =",str(message.payload.decode("utf-8")))

client= paho.Client("Client1") #create client object client1.on_publish = on_publish #assign function to callback client1.connect(broker,port) #establish connection client1.publish("house/bulb1","on")
######Bind function to callback
client.on_message=on_message
client.username_pw_set(username="test",password="test")
print("connecting to broker ",broker)
client.connect(broker,port)#connect
client.loop_start() #start loop to process received messages
print("subscribing ")
client.subscribe("TestTopic/lora/#")#subscribe
time.sleep(100)
# print("publishing ")
# client.publish("TestTopic/lora/","coucou")#publish
time.sleep(4)
client.disconnect() #disconnect
client.loop_stop() #stop loop