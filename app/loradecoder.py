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

import paho.mqtt.client as paho


# #############################################################################
#
# Global variables
# (scope: this file)
#

_condition          = None  # conditional variable used as interruptible timer
_shutdownEvent      = None  # signall across all threads to send stop event
_Cursor =  2 #Correspond a l'emplacement dans la payload, on initialise a 2 car les 2 premiers octet de la payload ne sont pas des datas donc la premiere data est a l'emplacement 3 dans la payload 


#Dictionnaire des types de data
TYPE =[
    {'nom':'analog_input',          'unit':'...',      'ID':1,  'size':1, 'mult':1},
    {'nom':'analog_output',         'unit':'...',      'ID':2,  'size':1, 'mult':1},
    {'nom':'digital_input',         'unit':'bool',      'ID':3,  'size':1, 'mult':1},
    {'nom':'digital_output',        'unit':'bool',      'ID':4,  'size':1, 'mult':1},
    {'nom':'luminosity',            'unit':'lux',      'ID':5,  'size':2, 'mult':1},
    {'nom':'presence',              'unit':'bool',      'ID':6,  'size':1, 'mult':1},
    {'nom':'frequency',             'unit':'pers/j',      'ID':7,  'size':2, 'mult':1},
    {'nom':'temperature',           'unit':'celcuis',      'ID':8,  'size':1, 'mult':100, 'ref':20, 'pas':0.25},
    {'nom':'humidity',              'unit':'%r.H',      'ID':9,  'size':1, 'mult':100, 'ref':0, 'pas':0.5},
    {'nom':'CO2',                   'unit':'ppm',      'ID':10, 'size':2, 'mult':1},
    {'nom':'air_quality',           'unit':'ppm',      'ID':11, 'size':1, 'mult':1},
    {'nom':'GPS',                   'unit':'...',      'ID':12, 'size':9, 'mult':1},
    {'nom':'energy',                'unit':'W/m2',      'ID':13, 'size':3, 'mult':1},
    {'nom':'UV',                    'unit':'W/m2',      'ID':14, 'size':3, 'mult':1},
    {'nom':'weight',                'unit':'g',      'ID':15, 'size':3, 'mult':1},
    {'nom':'pressure',              'unit':'mBar',      'ID':16, 'size':1, 'mult':1, 'ref':990, 'pas':1},
    {'nom':'generic_sensor_unsi',   'unit':'...',      'ID':17, 'size':4, 'mult':1},
    {'nom':'generic_sensor_sign',   'unit':'...',      'ID':18, 'size':4, 'mult':1},
]


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



#*** Retourne une liste avec nom, size, mult, ref, pas d'un type de data ***
def infodata (data_type):
    #data_type : est un eniter qui correspond au type de la data d'apres la convention cayenne 

    print ("datatype :%d" % data_type)
    for ind in TYPE : #on parcour le dictionnaire TYPE et on regarde si data_type correspond a une ID connu 
        global _Cursor
        if ind['ID'] == data_type :

            if 'ref' in ind :
                info = [ind['nom'],ind['unit'],ind['size'],ind['mult'],ind['ref'],ind['pas']]

            else:
                info = [ind['nom'],ind['unit'],ind['size'],ind['mult']]

            # _Cursor += ind['size'] + 1  #car on a dans la payload type, channel, data 
            return info 
    print("Pas bon type de data!!!")
    return False


#*** Transforme les datas de la convention cayenne en float
def transfo_data (info,data):
    #info : est la liste renvoye par infodata qui contien nom, size, mult, ref, pas d'un type de data 
    #data : est la data un tableau qui represente la data sous forme cayenne
    #DATA : est la data sous forme de float 

    # print ("in transfo data :%d ",data[0])
    if len(info) == 4 : #Les datas sans références
        if info[2] == 1: #La data est un binaire sur 1 octet
            DATA = data[0]

        elif info[2] == 2: #La data est un entier mis sur 2 octet
            DATA = float(data[0]+(data[1]<<8)) #LSB + MSB*256   

        elif info[2] == 3: #La data est un float mis sur 3 octet avec la partie entiere sur 2 octet et la partie float sur 1 octet 
            DATA = data[0]+(data[1]<<8) #LSB + MSB*256 ici partie entiere
            DATA += data[2]/256
            DATA = round(DATA,2) #Pour tronquer a 10^-2 

        elif info[2] == 4: #La data est un float mis sur 4 octet 
            DATA = data[0]+(data[1]<<8)+(data[2]<<16)+(data[3]<<24) 

    else :
        if info[0] == "temperature":
            # print("temp :%d"%data[0])
            pf = data[0]>>7

            if pf == 1 : #cas eniter negatif
                data=data[0] - (data[0]>>7)
                # print("temp :%d"%data)
                DATA = ((-data) * info[5]*100)/100 + info[4]

            else : #cas entier possitif
                DATA = (data[0] * info[5]*100)/100 + info[4]

        else :
            DATA = (data[0] * info[5]) + info[4]

    return DATA

#TODO faire des cas si il y a des pb !!!!

#*** Retourne la payload avec les informations du capteur pour MQTT
def decoder (PAYLOAD):
    #payload : la payload lora recut en MQTT
    #cursor : emplacement dans la payload du capteur
    #data : [data, unit]
    global _Cursor
    data = []
    row_data = []
    i = 0

    if _Cursor < len(PAYLOAD) :
        INFO = infodata(PAYLOAD[_Cursor])
        _Cursor += 2 #+2 car les datas sont sous la forme : type, channel, data
        while i < INFO[2] :
            row_data.append(PAYLOAD[_Cursor]) 
            # print("Cursor :%d"%_Cursor)
            # print ("row data[%d] :%d" %(i,row_data[i]))
            _Cursor += 1
            i += 1 

        data.append(transfo_data(INFO,row_data))
        data.append(INFO[1])

    else :
        print("PB en _Cursor dehors de payload dans decoder")
        return False
    
    return data



def Senso_campus(UID):
    #demande a Senso_campus les info par rappor à un uID
    return 0


#Va envoyer le message avec la data et l'unit de la data dans le bon topic MQTT(Pour le test ça sera /command)
def PUBLISH(payload, data):
    uID = payload["appargs"] #TODO a verifier !!!
    #TODO demander a Senso campus quelles est le site, le batiment et la salle de cet uID
    topic ="TestTopic/lora/"+uID+"/command" #donner par senso campus
    publish_payl = json.dumps({'unitID': uID, 'value': data[0], 'value_units': data[1]}, sort_keys=True)
    mqtt_client.publish(topic,publish_payl)#publish


#transforme la liste de char en liste de valeur hexa utilisable par le decoder
def str_to_int(payload):
    if len(payload)%2 == 1:#Si la liste na pas un nombre d'elements pair alors il y a un pb
        print("PB payload n'a pas un nombre d'éléments pair dans str_to_hex")
        return 0
    else :
        cursor = 0
        new_payl = []
        while cursor < len(payload) :
            new_payl.append(int("0X"+payload[cursor]+payload[cursor+1],16)) #on regroupe les deux elements de payload 
            cursor +=2
        return new_payl

def myMsgHandler(topic, payload):
    time.sleep(1)
    log.debug("MSG topic '%s' received ..." % str(topic) )
    if 'data' in payload :
        payl= payload["data"] #recupere seulement le champ data du message 
        print(payl)
        int_payl = str_to_int(payl)
        print(int_payl)
        if int_payl[0] == 0x01:
            while _Cursor < len(int_payl):
                data_dec=decoder(int_payl)
                print("Unit :%s"%data_dec[1])
                print("value final:%f"%data_dec[0])
                PUBLISH(payload,data_dec)



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
        
        # register own message handler
        client.handle_message = myMsgHandler
        client.send_message = PUBLISH

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

