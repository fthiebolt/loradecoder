# dataCOllector | docker container #

This repository holds the docker container app. that will grab both LIVE and OFFLINE data and to push them in our InfluxDB server.
Here are the following applications:

  - **application.py** is a Flask app
  - **legacy_collector.py** is the Hamdi's original dataCOllector that makes write to MongoDB
  - **datacollector.py** is the new dataCOllector that write down the data to InfluxDB
  - **metrics_collector.py** is the MQTT metrics collector that write down the data to InfluxDB

Notes:
  - directory **static** is intended to hold all of the statuc files (html,css etc) served as /static (e.g Bootstrap files)
*Note: /static files will get automagically served by uwsgi :) *


Original code from PhD Hamdi BenHamou 2017

