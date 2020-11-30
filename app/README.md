# loradecoder | docker container #

This repository holds the docker container app. that will deocode raw LoRaWAN messages sent by both neOCampus and eCOect end-devices.
Once decoded through our extended Cayenne LPP format decoder, messages will get republihed in proper MQTT topics according to our rules.

  - **application.py** is a Flask app
  - **loradecoder.py** is the LoRaWAN decoder main app.

Notes:

