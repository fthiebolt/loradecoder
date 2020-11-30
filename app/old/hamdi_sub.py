#!/usr/bin/env python3

import time, os
import paho.mqtt.client as mqtt
from datetime import datetime
import pymongo
import urllib
import json
class MongoCRUD:
    def __init__(self):
        
        print("connecting to the database")
    def get_db(self):
        from pymongo import MongoClient 
        password = urllib.quote_plus('sig@17!')
        client = MongoClient("mongodb://root2:"+password+"@127.0.0.1:27017")
        db = client.neocampus
        return db
    def get_data(self,db):
        return db.a.find()
        

class MQTTListener:
    hints = {}
    libvals ={}
    def __init__(self):
        self.db = MongoCRUD().get_db()
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message

    def start(self,libvals,hints):
        self.libvals = libvals 
        self.hints = hints 
        self.client.username_pw_set("neOCampus-U4", "neOCampus")
        self.client.connect("neocampus.univ-tlse3.fr", 1883, 60)
        self.client.loop_start()


    def on_connect(self, client, userdata, flags, rc):
        if rc != mqtt.MQTT_ERR_SUCCESS:
            print("[Capteurs] Fail to connect to ")
            self.client.reconnect()

        else:
            self.client.subscribe("u4/#")

    def on_disconnect(self, client, userdata, rc):
        # Network error
        if rc != mqtt.MQTT_ERR_SUCCESS:
            self.client.reconnect()

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print("#SUBSCRIBED : #" + str(client))

    def on_message(self, client, userdata, message):
        print("#MESSAGE : #" + str(client) + " #" + str(userdata) + " #" + str(message.topic) + " #" + message.payload.decode('utf-8'))
	#print(message.topic)
        
        #print(libvals)
        try:
            data = message.payload.decode("utf-8")
        
            x = json.loads(data)
            idm = db.measure.count()

#        if  "energy" not in message.topic and"subID" and "value_units"  in x.keys():
        
            key = (message.topic+"/"+str(x["unitID"])+"/"+str(x["subID"])+"") 
            
            #if('energy' not in message.topic):
            if(key in hints):
                #key = (message.topic+"/"+str(x["unitID"])+"/"+str(x["subID"])+"")
                idlibval = libvals[x["value_units"]]
                date = datetime.utcnow().isoformat()
                items = message.topic.split("/")
                #print(datetime.utcnow)
       
                db.measure.insert({"building": items[0] , "room":items[1] , "device" : items[2] ,"subId" : x["subID"] , "uri":message.topic+"/"+str(x["subID"]) ,"datemesure": datetime.utcnow() , "idMesure":idm , "idcapteur" : hints[key][0] , "idpiece" : hints[key][1] , "mesurevaleur" :[ {"idlibv":libvals[str(x["value_units"])], "valeur" : float(x["value"]) } ], "data":{"payload" : x,"date" : date , "uri" : message.topic+'/'+str(x["subID"]) }})
            
                idm +=1
            else:
                #key = (message.topic+"/"+str(x["unitID"])+"/"+str(x["subID"])+"")
                date = datetime.utcnow().isoformat()
                items = message.topic.split("/")
                db.measure.insert({"building": items[0] , "room":items[1] , "device" : items[2] ,"subId" : x["subID"] , "uri":message.topic+"/"+str(x["subID"]) ,"datemesure": datetime.utcnow() , "idMesure":idm  , "data":{"payload" : x,"date" : date , "uri" : message.topic+'/'+str(x["subID"]) }})
                idm+=1
        except:
            #print("#MESSAGE : #" + str(client) + " #" + str(userdata) + " #" + str(message.topic) + " #" + message.payload.decode('utf-8'))
            #db.temptest.insert({'date':datetime.utcnow()})
                db.failedData.insert({'topic': message.topic , 'date':datetime.utcnow(), 'payload':message.payload.decode('utf-8')})
        
#	print(message.payload.decode('utf-8'))
        #print("{} {}: {}".format(datetime.utcnow().isoformat(), message.topic, message.payload.decode('utf-8')))
        #path = "neoRecords/" + message.topic
        #now = datetime.utcnow().isoformat()
        #if not os.path.exists(path):
        #    os.makedirs(path)
        #with open(path + "/" + now + ".json", mode="w") as f:
        #    f.write(message.payload.decode('utf-8'))

if __name__ == "__main__":
    print("start")
    list = MQTTListener()
    DBCRUD = MongoCRUD()
    db = DBCRUD.get_db()
    now = str(datetime.now())
#    db.batiment.remove() 
#    db.batiment.insert({
#"adresse":"118 Route de Narbonne",
#"cp":"31400 ",
#"idBatiment":1,
#"nomBatiment":"U4",
#"pieces":
#[
#{
#"hist_capt":[
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":1},
#{
#"dateD": now,
#"dateF":None,
#"idCapteur":2},
#{"dateD":
#now,
#"dateF":None,
#"idCapteur":3},
#{"dateD":
#now,
#"dateF":None,
#"idCapteur":4},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":5},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":6},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":7},
#{"dateD":now,
#"dateF":None,
#"idCapteur":8},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":9},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":10},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":11},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":12},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":13},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":14},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":15}
#],
#"idPiece":1,
#"lat":43.56622850991506,
#"lng":1.4667096734046936,
#"nomP":"302"},
#{
#"hist_capt":[
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":16},
#{
#"dateD":now,
#"dateF":None,
#"idCapteur":17}],
#
#"idPiece":2,
#"lat":43.56622850991506,
#"lng":1.4667096734046936,
#"nomP":"CampusFab"}
#
# 
#],
# 
#"ville":"Toulouse"}
#)
#    db.typecapteur.remove()
#    db.typecapteur.insert(({
#    "Libvals":[{"description":"Temperature",
#                    "idLibVal":1,
#                    "libelle":"tmp",
#                    "ordre":0,
#                    "unite":"celsius"}],
#    "idTypeCapteur":1,
#    "nomType":"Temperature",
#    "Capteurs":
#
#        [ {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":2
#            },
#            "idCapteur":16,
#            "nomCapteur":"u4/campusfab/temperature/auto_92F8/79",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":2,
#                "nomP":"CampusFab"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":1,
#            "nomCapteur":"u4/302/temperature/inside/ilot1",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":2,
#            "nomCapteur":"u4/302/temperature/inside/ilot2",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":3,
#            "nomCapteur":"u4/302/temperature/inside/ilot3",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":4,
#            "nomCapteur":"u4/302/temperature/outside/ouest",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            }
#            ]
#                        }))
#    db.typecapteur.insert({
#    
#    "Capteurs":
#        [{"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":5,
#            "nomCapteur":"u4/302/co2/inside/ilot1",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":6,
#            "nomCapteur":"u4/302/co2/inside/ilot2",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":7,
#            "nomCapteur":"u4/302/co2/inside/ilot3",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            }
#            ],
#            "Libvals":[
#                {
#                    "description":"CO2",
#                    "idLibVal":2,
#                    "libelle":"co2",
#                    "ordre":None,
#                    "unite":"ppm"
#                }
#            ],
#            "idTypeCapteur":2,"nomType":"co2"})
#    db.typecapteur.insert({
#    
#    "Capteurs":
#        [{"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":17,
#            "nomCapteur":"u4/campusfab/luminosity/auto_2C76/57",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":2,
#                "nomP":"CampusFab"}
#                ]
#            },
#
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":8,
#            "nomCapteur":"u4/302/luminosity/inside/ilot1",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":9,
#            "nomCapteur":"u4/302/luminosity/inside/ilot2",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":10,
#            "nomCapteur":"u4/302/luminosity/inside/ilot3",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":11,
#            "nomCapteur":"u4/302/luminosity/outside/ouest",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            ],
#            "Libvals":[
#                {
#                    "description":"luminosity inside",
#                    "idLibVal":3,
#                    "libelle":"lum",
#                    "ordre":None,
#                    "unite":"lux"
#                },
#                {
#                    "description":"luminosity outside",
#                    "idLibVal":4,
#                    "libelle":"lum",
#                    "ordre":None,
#                    "unite":"w/m2"
#                }
#            ],
#            "idTypeCapteur":3,"nomType":"luminosity"})
#    db.typecapteur.insert({
#    
#    "Capteurs":
#        [{"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":12,
#            "nomCapteur":"u4/302/humidity/inside/ilot1",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":13,
#            "nomCapteur":"u4/302/humidity/inside/ilot2",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":14,
#            "nomCapteur":"u4/302/humidity/inside/ilot3",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            },
#            {"Piece_courante":
#            {
#                "dateD":now,
#                "idPiece":1
#            },
#            "idCapteur":15,
#            "nomCapteur":"u4/302/humidity/outside/ouest",
#            "nomreelcapteur":None,
#            "pieces":[
#                {"dateD":now,
#                "dateF":None,
#                "idPiece":1,
#                "nomP":"302"}
#                ]
#            }
#            ],
#            "Libvals":[
#                {
#                    "description":"humidity inside",
#                    "idLibVal":5,
#                    "libelle":"lum",
#                    "ordre":None,
#                    "unite":"%r.H."
#                },
#                
#            ],
#            "idTypeCapteur":4,"nomType":"humidity"})
    libvals = {}
    hints ={}
    for each in db.typecapteur.find():
        for inside in each["Capteurs"]:
            hints[inside["nomCapteur"]] = [inside["idCapteur"], inside["Piece_courante"]["idPiece"] ]
             
            #print(str(inside["idCapteur"] ),   str(inside["nomCapteur"]), inside["Piece_courante"]["idPiece"])
        for inner in each["Libvals"] :
             
            libvals[inner["unite"]] =  inner["idLibVal"]
    list.start(libvals,hints)
    while True:

       time.sleep(1) 



