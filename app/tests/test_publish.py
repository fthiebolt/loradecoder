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

def Senso_campus(UID):
    #demande a Senso_campus les info par rappor à un uID
    return 0


#Va envoyer le message avec la data et l'unit de la data dans le bon topic MQTT(Pour le test ça sera /command)
def PUBLISH(payload, data):
    uID = payload["uunitid"] #TODO a verifier !!!
    #TODO demander a Senso campus quelles est le site, le batiment et la salle de cet uID
    topic ="TestTopic/lora/command" #donner par senso campus
    publish_payl = json.dumps({'unitID': uID, 'value': data[0], 'value_units': data[1]}, sort_keys=True)
    client.publish(topic,publish_payl)#publish





def myMsgHandler(topic, payload):
    time.sleep(1)
    log.debug("MSG topic '%s' received ..." % str(topic) )
    # print( payload )
    payl= payload["data"] #recupere seulement le champ data du message 
    print(payl)
    while _Cursor < len(payl):
        data_dec=decoder(payl)
        print("Unit :%s"%data_dec[1])
        print("value final:%f"%data_dec[0])
        PUBLISH(payl,data_dec)
           




broker="neocampus.univ-tlse3.fr"
port=1883
#define callback

client= paho.Client("Client1") #create client object client1.on_publish = on_publish #assign function to callback client1.connect(broker,port) #establish connection client1.publish("house/bulb1","on")
######Bind function to callback
client.myMsgHandler=myMsgHandler
client.username_pw_set(username="...",password="...") #A changer pour le test 
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


# def main():
#     # a = infodata(8)
#     # print(a)
#     # # b=transfo_data(a,[0x00,0x00,0x99,0x42])
#     # b=transfo_data(a,1)
#     # print(b)

   



# if __name__ == "__main__":
#     main()
#     pass