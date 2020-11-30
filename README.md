# [neOCampus] dataCOllector: LIVE and OFFLINE data retriever #
______________________________________________________________

This app. is responsible to collect data from both live (MQTT) and offline (API) sources.

### Environment variables ###
When you start this application (see below), you can pass several environment variables:

  - **DEBUG=1** this is our application debug feature
  - **SIM=1** this is our application simulation feature: kind of *read-only* mode (i.e no write to any database)
  - **FLASK_DEBUG=1** this is debug to Flask internals
  - **FLASK_ENV=development** this is FLASK_DEBUG mode + automatic restart + ___
  - **MQTT_SERVER** and **MQTT_PORT**
  - **MQTT_USER** and **MQTT_PASSWD** are MQTT credentials
  - **MQTT_TOPICS** json formated list of topics to subscribe to
  - **MQTT_UNITID** is a neOCampus identifier fr msg filtering
  - **MONGO_USER** and **MONGO_PASSWD** are MongoDB credentials
  - **MONGO_SERVER**=172.17.0.1   this is the docker gateway
  - **MONGO_PORT**=27017
  - **MONGO_DATABASE**=neocampus    name of the database
  - **INFLUX_TOKEN**
  - **INFLUX_SERVER**=172.17.0.1
  - **INFLUX_PORT**=9999
  - **INFLUX_ORG**=neOCampus    name of the organization
  - **INFLUX_BUCKETS** is a list featuring name of 'sensors' and then 'inventory' buckets e.g [ 'ut3_sensors', 'ut3_inventory' ]


### [HTTP] git clone ###
Only **first time** operation.

`git clone https://fthiebolt@bitbucket.org/fthiebolt/datacollector.git`  
*it will then ask for a password*

### git pull ###
We'll make use of the embedded deployment key through a script
```
cd datacollector
git pull
```
This script will output 'Updated' along with exit code 0 if repository has been updated. Hence you'll need to restart associated scripts to code.

### git push ###
In order to be able to push changes, we switch to HTTPS git remote, hence you'll need the **account's password**.
```
cd datacollector
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
cd /neocampus/datacollector
FLASK_ENV=development FLASK_DEBUG=1 SIM=1 DEBUG=1 INFLUX_TOKEN='token' MQTT_PASSWD='passwd' MONGO_PASSWD='passwd' docker-compose up -d
```  

### fast update of existing running container ###
```
cd /neocampus/datacollector
git pull
DEBUG=1 INFLUX_TOKEN='token' MQTT_PASSWD='passwd' MONGO_PASSWD='passwd' docker-compose up --build -d
```  

### ONLY (re)generate image of container ###
```
cd /neocampus/datacollector
docker-compose --verbose build --force-rm --no-cache
[alternative] docker build --no-cache -t datacollector -f Dockerfile .
```

### start container for maintenance ###
```
cd /neocampus/datacollector
docker run -v /etc/localtime:/etc/localtime:ro -v "$(pwd)"/app:/opt/app:rw -it datacollector bash
```

### ssh root @ container ? ###
Yeah, sure like with any VM:
```
ssh -p xxxx root@locahost
```  

