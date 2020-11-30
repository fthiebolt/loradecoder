# [neOCampus] loradecoder: LoRaWAN msg decoder #
______________________________________________________________

This app. is responsible for decoding LoRaWAN messages from the neOCampus LoRaWAN-server.
It collects raw LoRaWAN frames from LORA_TOPIC, it decodes the payload (extended Cayenne LLC format).
Once decoded, localized (in conjunction with sensOCampus), the message is (re)published in our MQTT topics
to finally get collected from the neOCampus dataCOllector.

### Environment variables ###
When you start this application (see below), you can pass several environment variables:

  - **DEBUG=1** this is our application debug feature
  - **SIM=1** this is our application simulation feature: kind of *read-only* mode (i.e no write to any database)
  - **FLASK_DEBUG=1** this is debug to Flask internals
  - **FLASK_ENV=development** this is FLASK_DEBUG mode + automatic restart + ___
  - **MQTT_SERVER** and **MQTT_PORT**
  - **MQTT_USER** and **MQTT_PASSWD** are MQTT credentials
  - **MQTT_TOPICS** json formated list of topics to subscribe to
  - **MQTT_UNITID** is a neOCampus identifier for msg filtering


### [HTTP] git clone ###
Only **first time** operation.

`git clone https://github.com/fthiebolt/loradecoder.git`  

### git pull ###
```
cd loradecoder
git pull
```

### git push ###
```
cd loradecoder
./git-push.sh
```

**detached head case**
To commit mods to a detached head (because you forget to pull head mods before undertaking your own mods)
```
cd <submodule>
git branch tmp
git checkout master
git merge tmp
git branch -d tmp
```

### start container ###
```
cd /neocampus/loradecoder
FLASK_ENV=development FLASK_DEBUG=1 SIM=1 DEBUG=1 MQTT_PASSWD='passwd' docker-compose up -d
```  

### fast update of existing running container ###
```
cd /neocampus/loradecoder
git pull
DEBUG=1 INFLUX_TOKEN='token' MQTT_PASSWD='passwd' docker-compose up --build -d
```  

### ONLY (re)generate image of container ###
```
cd /neocampus/loradecoder
docker-compose --verbose build --force-rm --no-cache
[alternative] docker build --no-cache -t loradecoder -f Dockerfile .
```

### start container for maintenance ###
```
cd /neocampus/loradecoder
docker run -v /etc/localtime:/etc/localtime:ro -v "$(pwd)"/app:/opt/app:rw -it loradecoder bash
```

### ssh root @ container ? ###
Yeah, sure like with any VM:
```
ssh -p xxxx root@locahost
```  

