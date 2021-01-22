#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# High-level MQTT management module
#
# [mar.20] F.Thiebolt   added support to multiple topics to subscribe to
# [jan.20] F.Thiebolt   adapted for the weather agent app.
# [nov.19] F.Thiebolt   add on_log messages
# [sep.17] F.Thiebolt   extending support for MQTT errors
# [apr.17] F.Thiebolt   started to add support for MQTT errors
# [May.16] T.Bueno      initial release
#



# #############################################################################
#
# Import zone
#
import os
import sys
import time
import json
from threading import Thread, Event
import paho.mqtt.client as mqtt_client
from random import randint


# --- project related imports
import settings
from logger.logger import log, getLogLevel



# #############################################################################
#
# Class
#
class CommModule(Thread):

    # class attributes ( __class__.<attr_name> )

    # objects attributes
    sim             = False     # read-only mode (i.e no publish)
    _connection     = None      # mqtt client
    _connected      = False
    _mqtt_user      = None
    _mqtt_passwd    = None
    _mqtt_topics    = None      # list of topics to subscribe to
    _unitID         = None
    _addons         = None      # additional parameters


    #
    # object initialization
    def __init__(self, mqtt_user, mqtt_passwd, mqtt_topics, *args, **kwargs ):
        super().__init__()

        log.debug("initializing comm module")
        self._mqtt_user     = mqtt_user
        self._mqtt_passwd   = mqtt_passwd
        self._mqtt_topics   = mqtt_topics
        self._addons        = kwargs

        # check for _shutdown event
        self._shutdownEvent = self._addons.get('_shutdownEvent')
        if( self._shutdownEvent is None ):
            log.warning("unspecified global shutdown ... thus locally specified ...")
            self._shutdownEvent = Event()

        # check for simulator mode (i.e read-only mode)
        self.sim = self._addons.get('sim',False)
        if( self.sim is True ):
            log.info("[SIM] read-only mode ACTIVATED ... means NO PUBLISH AT ALL!")

        # check for unitID
        if( "unitID" in self._addons and self._addons.get('unitID') is not None ):
            self._unitID = self._addons.get('unitID')

        # setup MQTT connection
        self._connection = mqtt_client.Client()
        self._connection.on_connect = self._on_connect
        self._connection.on_disconnect = self._on_disconnect
        self._connection.on_publish = self._on_publish
        self._connection.on_message = self._on_message
        self._connection.on_subscribe = self._on_subscribe
        self._connection.on_unsubscribe = self._on_unsubscribe
        self._connection.on_log = self._on_log

        self._connection.reconnect_delay_set( min_delay=settings.MQTT_RECONNECT_DELAY )
        self._connection.username_pw_set( self._mqtt_user, self._mqtt_passwd )

        self._connected = False
        log.debug("initialization done")


    #
    # called by Threading.start()
    def run( self ):
        # load module
        log.info("module loading")
        self.load()

        # start connection
        log.info("start MQTT connection to '%s:%d' ..." % (self._addons['mqtt_server'],self._addons['mqtt_port']))
        self._connection.connect( host=self._addons['mqtt_server'], port=self._addons['mqtt_port'], keepalive=settings.MQTT_KEEP_ALIVE )

        # launch
        try:
            while not self._shutdownEvent.is_set():

                if self._connection.loop(timeout=2.0) != mqtt_client.MQTT_ERR_SUCCESS:
                    log.debug("loop failed, sleeping a bit before retrying")
                    time.sleep(2)

            log.debug("shutdown activated ...")

        except Exception as ex:
            if getLogLevel().lower() == "debug":
                log.error("module crashed (high details): " + str(ex), exc_info=True)
            else:
                log.error("module crashed: " + str(ex))

        # shutdown module
        log.info("module stopping")
        self.quit()

        # disconnect ...
        self._connection.disconnect()

        # end of thread
        log.info("Thread end ...")


    ''' are we connected ? '''
    def is_connected( self ):
        return self._connected


    ''' prepares and sends a payload in a MQTT message '''
    def send_message(self,topic, payload):

        # uID = payload["appargs"]
        # #TODO demander a Senso campus quelles est le site, le batiment et la salle de cet uID
        # topic ="TestTopic/lora/"+uID+"/command" #donner par senso campus
        # publish_payl = json.dumps({'unitID': uID, 'value': data[0], 'value_units': data[1]}, sort_keys=True)
        # # client.publish(topic,publish_payl)#publish
        # res, mid = self._connection.publish(topic,publish_payl)
        # if not self.is_connected():
        #     log.warn("tried to publish a message while not connected ...")
        #     return

        # if 'unitID' not in payload:
        #     payload['unitID'] = self._unitID

        # if( payload['unitID'] is None ):
        #     log.warn("tried to publish a message while not having a unitID ... aborting")
        #     return

        # if( self.sim is True ):
        #     return

        # res, mid = self._connection.publish(topic, json.dumps(payload))

        # if res != mqtt_client.MQTT_ERR_SUCCESS:
        #     log.error("on message published to topic " + topic)
        pass


    ''' handles pre-validated MQTT messages, to be implemented by subclasses '''
    def handle_message(self, topic, payload):
        pass


    ''' load method, to initialize modules before running, to be implemented by subclasses '''
    def load(self):
        pass


    ''' quit method, to shutdown modules before exiting, to be implemented by subclasses '''
    def quit(self):
        pass


    # -------------------------------------------------------------------------
    # Low-level functions
    # - _on_connect()
    # - _on_disconnect()
    # - _on_publish()
    # - _on_subscribe()
    # - _on_unsubscribe()
    # - _on_message()
    # - _on_log()
    # - _status()
    #


    ''' paho callback for connection '''
    def _on_connect(self, client, userdata, flags, rc):

        if rc != mqtt_client.MQTT_ERR_SUCCESS:
            log.error("unable to connect to broker '%s:%d': " % (self._addons['mqtt_server'],self._addons['mqtt_port']) + mqtt_client.error_string(rc))
            return

        log.info("connected to broker :)")
        self._connected = True

        # subscribe to topics list
        try:
            for topic in self._mqtt_topics:
                log.debug("subscribing to " + str(topic))
                self._connection.subscribe( topic )   # QoS=0 default

        except Exception as ex:
            log.warn("exception while subscribing to topic '%s' :" % str(topic) + str(ex))


    ''' paho callback for disconnection '''
    def _on_disconnect(self, client, userdata, rc):

        log.info("disconnected from MQTT broker with rc: " + mqtt_client.error_string(rc))
        self._connected = False
        if rc == mqtt_client.MQTT_ERR_SUCCESS:
            # means that disconnect has been requested (i.e not an unexpected event)
            return

        # unexpected disconnect ... we'll retry till death ...
        _time2sleep = randint( settings.MQTT_RECONNECT_DELAY, settings.MQTT_RECONNECT_DELAY**2 )
        while( rc != mqtt_client.MQTT_ERR_SUCCESS and self._shutdownEvent.is_set() is not True):
            if (_time2sleep > 300):     # max. 5mn between two retrials
                _time2sleep = 300
            log.info("Unexpected disconnection ... sleeping %d seconds before retrying" % _time2sleep)
            time.sleep(_time2sleep)
            log.info("... trying to reconnect ...")
            try:
                rc = self._connection.reconnect()
            except Exception as ex:
                log.info("caught exception while mqtt reconnect: " + str(ex) )
                rc = -1
            _time2sleep = _time2sleep*2


    ''' paho callback for published message '''
    def _on_publish(self, client, userdata, mid):
        log.debug("mid: " + str(mid)+ " published!")


    ''' paho callback for message reception '''
    def _on_message(self, client, userdata, msg):

        #log.debug("receiving a msg on topic '%s' ..." % str(msg.topic) )
        try:
            # loading and verifying payload
            payload = json.loads(msg.payload.decode('utf-8'))
            #validictory.validate(payload, self.COMMAND_SCHEMA)
        except Exception as ex:
            log.error("exception handling json payload from topic '%s': " % str(msg.topic) + str(ex))
            return

        # is it a message for us ??
        if( self._unitID is not None and payload['dest'] != "all" and payload['dest'] != str(self.unitID) ):
            log.debug("msg received on topic '%s' features destID='%s' != self._unitID='%s'" % (str(msg.topic),payload['dest'],self.unitID) )
            return

        self.handle_message( msg.topic, payload )


    ''' paho callback for topic subscriptions '''
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        log.debug("Subscribed: " + str(mid) + " " + str(granted_qos))
        self._connected = True


    ''' paho callback for topic unsubscriptions '''
    def _on_unsubscribe(self, client, userdata, mid):
        pass


    # [nov.19] Francois
    def _on_log(self, client, userdata, level, buf):
        ''' print exception that may occur in callbacks '''
        # only printing ERR and WARN
        if( level == mqtt_client.MQTT_LOG_ERR or
            level == mqtt_client.MQTT_LOG_WARNING ):
            print("[on_log][%s] %s" % (str(level),str(buf)))


    ''' Low -level module'status reporting, to be implemented by subclasses '''
    def _status(self):
        ''' Raw status used both by module's reporting and higher-level device reporting '''
        return None

