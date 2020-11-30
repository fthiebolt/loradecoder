#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# neOCampus InfluxDB management module for:
#   - neOCampus data @ InfluxDB
#   - neOCampus mqtt metrics @ InfluxDB
#
# [Jun.20] F.Thiebolt   added support for non existing tags in _buildQuery
#                       added support for queryAddon in find method
# [Apr.20] F.Thiebolt   initial release
#



# #############################################################################
#
# Import zone
#
import os
import sys
import time
import json
from datetime import datetime,timezone,timedelta
from threading import Event
from random import randint

# InfluxDB client --> https://pypi.org/project/influxdb-client/
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- project related imports
from logger.logger import log, getLogLevel
import settings



# #############################################################################
#
# Class
#
class InfluxModule(object):
    ''' InfluxDB facility module:
        handle everything related to the InfluxDB database
    '''

    #
    # class attributes ( __class__.<attr_name> )
    DEFAULT_LOCATION    = 'ut3'     # when no information is provided about site location

    # InfluxDB time precisions
    INFLUX_SENSORS_PRECISION    = WritePrecision.S
    #INFLUX_METRICS_PRECISION    = WritePrecision.MN    # [apr.20] minute precision does not yet exist

    # Inventory bucket related attributes
    #   name of measurements
    INVENTORY_SITES     = 'location'
    INVENTORY_BUILDINGS = 'building'
    INVENTORY_ROOMS     = 'room'
    INVENTORY_KINDS     = 'kind'

    # Sensors bucket related attributes
    MEASUREMENT_SENSORS = 'data'
    # Metrics bucket related attributes
    MEASUREMENT_METRICS = 'mqtt'


    #
    # objects attributes
    sim                 = False # read-only mode (i.e no publish)

    _client             = None  # database client
    _writeAPI           = None
    _queryAPI           = None
    _deleteAPI          = None

    _influx_token       = None
    _influx_server      = None
    _influx_port        = None
    _influx_org         = None
    _bucket_sensors     = None  # bucket for data sensors values
    _bucket_inventory   = None  # bucket for sensors related inventories (e.g building, rooms etc)
    _bucket_metrics     = None  # bucket for metrics (e.g mqtt)

    _addons             = None  # additional parameters
    
    _URIsensorsDict     = None  # dictionnary of sensors' URI: { '<location>/<building>/<room/<unitID>/<subID>': True }
                                # at startup, we read 'inventory' bucket to build this dictionnary of URIs
                                # dict complexity is O(1) ;)


    #
    # object initialization
    def __init__( self, influx_token, influx_server, influx_port, influx_org, *args, **kwargs ):

        log.debug("initializing influxDB module")

        self._influx_token          = influx_token
        self._influx_server         = influx_server
        self._influx_port           = influx_port
        self._influx_org            = influx_org
        self._addons                = kwargs

        # check for _shutdown event
        self._shutdownEvent = self._addons.get('_shutdownEvent')
        if( self._shutdownEvent is None ):
            log.warning("unspecified global shutdown ... thus locally specified ...")
            self._shutdownEvent = Event()

        # check for simulator mode (i.e read-only mode)
        self.sim = self._addons.get('sim',False)
        if( self.sim is True ):
            log.info("[SIM] read-only mode ACTIVATED ...")

        # check for bucket about sensors data
        self._bucket_sensors    = self._addons.get('bucket_sensors')
        # check for optional bucket about sensors related inventories
        self._bucket_inventory  = self._addons.get('bucket_inventory')
        # check for bucket about metrics
        self._bucket_metrics    = self._addons.get('bucket_metrics')
        # ... at least one bucket defined
        if( self._bucket_sensors is None and self._bucket_metrics is None ):
            log.error("at least a sensors or metrics bucket ought to get defined !")
            raise Exception("at least a sensors or metrics bucket ought to get defined !")


        # setup database connection
        self._connect()
            
        # build sensors URI dict from database
        self.getSensorsURI()

        # display status
        self.status()

        log.debug("initialization done")


    def __repr__( self ):
        ret = "\n"
        ret += "=== InfluxDB connexion ===" + '\n'
        ret += "=== InfluxDB organization: %s" % self._influx_org + '\n'
        ret += "=== Data sensors bucket: %s" % self._bucket_sensors + '\n'
        ret += "=== Sensors inventory bucket: %s" % self._bucket_inventory + '\n'
        ret += "=== Metrics bucket: %s" % self._bucket_metrics + '\n'
        return ret


    def clearBucket( self, bucket, start, stop, *args, **kwargs ):
        ''' function to clear a bucket for a specific time range.
            optional argument: delete_filter
        '''

        # REFUSE to delete sensors bucket
        if( bucket == self._bucket_sensors ):
            raise Exception("sensors bucket DELETION REFUSED!")


        print(f"\n\n\tWARNING: you're about to DELETE DATA:\n\tbucket {bucket}: {start} --> {stop}")
        print("\tpress CTRL-C to cancel ...")
        for _ in range(8):
            print('.',end='')
            time.sleep(0.5)
            if( self._shutdownEvent.is_set() is True ):
                log.info("operation cancelled!")
                return

        log.warning(f"starting DATA DELETION from bucket {bucket}: {start} --> {stop}")

        # RFC-3339 start
        if( isinstance( start, datetime) ):
            _start_date = start.isoformat()        
        else:
            _start_date = start
        if( _start_date.endswith('Z') is not True ):
            _start_date += 'Z'

        # RFC-3339 stop
        if( isinstance( stop, datetime) ):
            _stop_date = stop.isoformat()        
        else:
            _stop_date = stop
        if( _stop_date.endswith('Z') is not True ):
            _stop_date += 'Z'

        # SIMULATION mode ?
        if( self.sim is True ):
            log.debug("[SIM] read-only mode active: no database deletion ...")
            return

        # now delete data from bucket
        try:
            self._deleteAPI.delete( _start_date,
                                    _stop_date,
                                    kwargs.get('delete_filter',""),
                                    bucket=bucket,
                                    org=self._influx_org)

            log.info(f"bucket's data DELETED!")
            time.sleep(1)
        except Exception as ex:
            log.warning("exception detected while inserting measure: " + str(ex) )
            raise

        # tada ! :)
        return True


    def is_connected( self ):
        ''' are we connected ? '''
        return True if self._client is not None else False


    def getSensorsURI( self ):
        ''' get URIs from inventory bicket '''

        if( self._bucket_inventory is None or not len(self._bucket_inventory) ):
            log.info("no inventory bucket provided ... continuing without")
            return

        if( self._URIsensorsDict is None ):
            self._URIsensorsDict =  dict()

        #
        # TODO: parse measures from 'inventory' bucket
        #


    def addSensorsURI( self, uriDict ):
        ''' check for new building, room etc; update local cache and DB is needed '''

        pass
        #
        # TODO: compare uriDict with local cache
        #       update DB is new item



    ''' send back status '''
    def status( self ):
        log.info("=== InfluxDB connexion ===")
        log.info("=== InfluxDB organization: %s" % self._influx_org)
        log.info("=== Data sensors bucket: %s" % self._bucket_sensors)
        log.info("=== Sensors inventory bucket: %s" % self._bucket_inventory)
        log.info("=== Metrics bucket: %s" % self._bucket_metrics)
        #TODO: return buckets status along with connectivity



    ''' handle received metrics MQTT message '''
    def msgHandler_metrics( self, topic, payload, *args, **kwargs ):
        ''' function called whenever our MQTT client receive metrics data.
            Beware that it's called by mqtt_loop's thread !
        '''

        # check for special topics we're not interested in
        if( not topic.startswith('$SYS') ):
            log.debug("[NOT A METRICS] special topic not for us: %s" % str(topic) )
            return

        # timestamp
        _dataTime = datetime.now(timezone.utc)
        # [apr.20] lowering precision to minute because WritePrecision.MN does not yet exist :|
        _dataTime = _dataTime.replace(second=0,microsecond=0)

        # Retrieve uri items as a dictionnary
        #   - for sensors <location/building/room/kind/unitID/subID>
        #   - for metrics <direction/kind/avg>
        try:
            _uriDict = self._extract_uri( topic, payload )
        except Exception as ex:
            log.error("unable to extract uri from topic='%s' and payload='%s': " % (str(topic),str(payload)) + str(ex))
            return

        # build InfluxDB point
        # <measure>,<tag1>=<key1>,<tag2>=<key2> <flag1>=<val1>,<flag2>=<val2> <timestamp>
        point = Point(__class__.MEASUREMENT_METRICS)

        # ... then add sorted TAGS
        # Remember: TAGS in lexicographic order !!
        for _elem in sorted(_uriDict.items()):
            if( _elem[1] is None): continue
            point.tag(_elem[0],_elem[1])

        # value FIELD
        point.field('value', float(payload) )

        # value_units FIELD
        point.field('value_units', _uriDict.get('kind'))    # 'messages' or 'bytes'

        # ... finally add timestamp
        try:
            # [apr.20] precision set to minute because WritePrecision.MN does not yet exist :|
            #point.time( _dataTime, write_precision=__class__.INFLUX_METRICS_PRECISION )
            point.time( _dataTime, write_precision=__class__.INFLUX_SENSORS_PRECISION )

        except Exception as ex:
            log.warning("exception detected while setting timestamp " + str(_dataTime) + " :" + str(ex))
            return

        if( getLogLevel().lower() == "debug" or self.sim is True ):
            print( point.to_line_protocol() )


        # SIMULATION mode ?
        if( self.sim is True ):
            log.debug("[SIM] read-only mode active: no database writings")
            return

        # now write data to bucket
        try:
            # [apr.20] WritePrecision.MN does not yet exist :|
            #self._writeAPI.write( bucket=self._bucket_metrics, record=point, write_precision=__class__.INFLUX_METRICS_PRECISION )
            self._writeAPI.write( bucket=self._bucket_metrics, record=point, write_precision=__class__.INFLUX_SENSORS_PRECISION )

        except Exception as ex:
            log.warning("exception detected while inserting measure: " + str(ex) )

        # tada ! :)
        return True




    ''' handle received MQTT message
        Note: may also get used for offline data ingestion, hence the additional 'timestamp' parameter
    '''
    def msgHandler( self, topic, payload, *args, **kwargs ):
        ''' function called whenever our MQTT client receive sensors data.
            Beware that it's called by mqtt_loop's thread !
            return True is data has been properly written, False if error/exception, None otherwise
        '''

        # check for special topics we're not interested in
        if( topic.startswith('_') or topic.startswith('TestTopic') or "camera" in topic
            or "access" in topic or "display" in topic or "attendance" in topic
            or topic.endswith('command') ):
            log.debug("[NOT A SENSOR] special topic not for us: %s" % str(topic) )
            return None

        log.debug("MSG topic '%s' received ..." % str(topic) )
        if getLogLevel().lower() == "debug":
            print( payload )


        # timestamp could be:
        # - argument to this function (expected datetime structure)
        # - within payload
        # - locally generated
        # Note: if timestamp is locally generated ==> a priori no possible data duplicate
        #   ... otherwise we need to check ...
        _checkDataInsertion = True
        _dataTime = kwargs.get('timestamp')
        if( _dataTime is None ):
            for _key in settings.MQTT_PAYLOAD_TIMESTAMPS:
                if( _key in payload.keys() ):
                    _dataTime = payload.get(_key)
                    break

        if( _dataTime is None ):
            _checkDataInsertion = False
            _dataTime = datetime.now(timezone.utc)
        elif( kwargs.get('forceNoDuplicateCheck') is True ):
            log.debug("force flag not to check for duplicate activated ...")
            _checkDataInsertion = False


        # Retrieve uri items <location/building/room/kind/unitID/subID>
        # as a dictionnary
        try:
            _uriDict = self._extract_uri( topic, payload )
        except Exception as ex:
            log.error("unable to extract uri from topic='%s' and payload='%s': " % (str(topic),str(payload)) + str(ex))
            return False

        # Check for new building, room etc etc
        #   automatic cache update and DB if needed
        self.addSensorsURI( _uriDict )

        # build InfluxDB point
        # <measure>,<tag1>=<key1>,<tag2>=<key2> <flag1>=<val1>,<flag2>=<val2> <timestamp>
        point = Point(__class__.MEASUREMENT_SENSORS)

        # ... then add sorted TAGS
        # Remember: TAGS in lexicographic order !!
        for _elem in sorted(_uriDict.items()):
            if( _elem[1] is None): continue
            point.tag(_elem[0],_elem[1])


        # 'value' FIELD
        _f_value = payload.get('value')
        if( _f_value is None ):
            # kind == SHUTTER
            if( _uriDict['kind'].lower() == "shutter" and "status" in payload.keys() 
                and "order" in payload.keys() and payload.get('order').lower() == "stop" ):
                _f_value = payload.get('status') 
            else:
                # no others know message without key 'value' ...
                return None

        _field = None
        #print(f"f_value = {_f_value}")

        # import value as FLOAT
        if( _field is None and isinstance(_f_value,float) ):
            try:
                _field = float(_f_value)
                #print("FLOAT detected")
            except Exception as ex:
                _field = None

        # import value as INT
        if( _field is None and isinstance(_f_value,int) ):
            try:
                _field = int(_f_value)
                #print("INT detected")
            except Exception as ex:
                _field = None

        # check if value is a string
        if( _field is None and isinstance(_f_value,str) ):
            _field = _f_value
            #print("STR detected")

        if( _field is None ):
            # ok, value is neither an int, float nor string ... maybe a list or a dictionnary ==> JSON
            # Note: to retrieve the original structure --> json.loads(<string>)
            _field = json.dumps( _f_value )
            #print("LIST/DICT/??? detected")

        # OVERRIDE:
        # [may.20] force 'value' as float (if needed)
        if( (_uriDict['kind']=='temperature' or _uriDict['kind']=='pressure') and
            _field is not None and isinstance(_field,float) is not True ):
            log.warning(f"{_uriDict} force _value as float")
            _field = float(_f_value)
        # [may.20] force 'value' as int (if needed)
        if( _uriDict['kind']=='humidity' and _field is not None and isinstance(_field,int) is not True ):
            log.warning(f"{_uriDict} force _value as int")
            _field = int(_f_value)

        point.field('value', _field)


        # 'value_units' FIELD
        _f_value_units = payload.get('value_units')
        if( _f_value_units is None ):
            # kind == DIGITAL
            if( _uriDict['kind'].lower() == "digital" and "type" in payload.keys() ):
                _f_value_units = payload.get('type') 
            # kind == SHUTTER
            if( _uriDict['kind'].lower() == "shutter" ):
                _f_value = 'position' 

        if( _f_value_units is not None ):
            _field = None
            if( isinstance(_f_value_units,str) is True ):
                _field = _f_value_units
            else:
                # ok, value_units is not a string ... maybe a list or a dictionnary ==> JSON
                # Note: to retrieve the original structure --> json.loads(<string>)
                _field = json.dumps( _f_value_units )

            point.field('value_units', _field)


        # ... finally add timestamp
        #   precision: second (not ns)
        try:
            point.time( _dataTime, write_precision=__class__.INFLUX_SENSORS_PRECISION )
        except Exception as ex:
            log.warning("exception detected while setting timestamp " + str(_dataTime) + " :" + str(ex))
            return False

        if( getLogLevel().lower() == "debug" or self.sim is True ):
            print( point.to_line_protocol() )

        # check for duplicate ?
        if( _checkDataInsertion is True ):
            if( self._is_duplicate( point=point ) is True ):
                log.debug("Point is duplicate ... cancel insertion")
                return None

        # SIMULATION mode ?
        if( self.sim ):
            log.info("[SIM] read-only mode active: no database writings")
            return None

        # now write data to bucket
        try:
            self._writeAPI.write( bucket=self._bucket_sensors, record=point, write_precision=__class__.INFLUX_SENSORS_PRECISION )

        except Exception as ex:
            log.error(f"exception detected while inserting measure from topic '{topic}':" + str(ex) )
            print(f"Topic: {topic}  Payload: {payload}  Point: {point.to_line_protocol()}")
            return False


        # tada ! :)
        return True



    ''' function to return an iterator over a specified range according to an optional query '''
    def find( self, start_date, end_date=None, interval=None, tags=None, fields=None, *args, **kwargs ):
        ''' start_date: str or datetime, end_date: str or datetime, interval: int (seconds)
            tags: dict, fields: list
        '''

        if( end_date is None and interval is None ): return None

        # _startTime
        if( isinstance(start_date, str) ):
            _startTime = datetime.fromisoformat( start_date )
        elif( isinstance(start_date, datetime) ):
            _startTime = start_date
        else:
            log.error("unsupported start_date format !")
            return None
        _startTime = _startTime.replace(microsecond=0)
        
        # _endTime
        if( end_date is not None ):
            if( isinstance(end_date, str) ):
                _endTime = datetime.fromisoformat( end_date )
            elif( isinstance(end_date, datetime) ):
                _endTime = end_date
            else:
                log.error("unsupported end_date !")
                return None
            _endTime = _endTime.replace(microsecond=0)
        else:
            _endTime = _startTime + timedelta(seconds=abs(int(interval)))


        # retrieve Flux query
        _query = self._buildQuery( bucket=self._bucket_sensors if kwargs.get('bucket') is None else kwargs.get('bucket'),
                                    measurement=__class__.MEASUREMENT_SENSORS,
                                    startTime=_startTime,
                                    endTime=_endTime,
                                    tags=tags,
                                    fields=fields )
        if( kwargs.get('query_addon') is not None ):
            _query += kwargs.get('query_addon')

        if getLogLevel().lower() == "debug":
            print(_query)


        # return stream
        return self._queryAPI.query_stream( _query )


    ''' function to return a pandas data frame according to a specified time range '''
    def get_df( self, start_date, end_date=None, interval=None, tags=None, fields=None, *args, **kwargs):
        ''' start_date: str or datetime, end_date: str or datetime, interval: int (seconds)
            tags: dict, fields: list
            bucket: str     optional bucket name if not default one
            querry_addon: str  are additionnal flux commands appended to the end of the query
        '''

        if( end_date is None and interval is None ): return None

        # _startTime
        if( isinstance(start_date, str) ):
            _startTime = datetime.fromisoformat( start_date )
        elif( isinstance(start_date, datetime) ):
            _startTime = start_date
        else:
            log.error("unsupported start_date format !")
            return None
        _startTime = _startTime.replace(microsecond=0)
        
        # _endTime
        if( end_date is not None ):
            if( isinstance(end_date, str) ):
                _endTime = datetime.fromisoformat( end_date )
            elif( isinstance(end_date, datetime) ):
                _endTime = end_date
            else:
                log.error("unsupported end_date !")
                return None
            _endTime = _endTime.replace(microsecond=0)
        else:
            _endTime = _startTime + timedelta(seconds=abs(int(interval)))

        # retrieve Flux query
        _query = self._buildQuery( bucket=self._bucket_sensors if kwargs.get('bucket') is None else kwargs.get('bucket'),
                                    measurement=__class__.MEASUREMENT_SENSORS,
                                    startTime=_startTime,
                                    endTime=_endTime,
                                    tags=tags,
                                    fields=fields )
        if( kwargs.get('query_addon') is not None ):
            _query += kwargs.get('query_addon')

        if getLogLevel().lower() == "debug":
            print(_query)


        # return pandas data frame
        return self._queryAPI.query_data_frame( _query )


    ''' function to write a pandas data frame '''
    def write_df( self, df, tags, *args, **kwargs):
        ''' all others columns not specified as TAGS will become FIELDS
            kwargs: bucket,
        '''

        # bucket
        _bucket = kwargs.get('bucket',self._bucket_sensors)

        # measurement
        _measurement = __class__.MEASUREMENT_SENSORS

        # tags
        if( tags is None or len(tags)==0 ):
            raise Exception("TAGS list for writing dataframes OUGHT to get defined")


        if( getLogLevel().lower() == "debug" or self.sim is True ):
            print(f"[write_DF] target bucket: {_bucket}\n{df}" )


        # SIMULATION mode ?
        if( self.sim is True ):
            log.debug("[SIM] read-only mode active: no database writings")
            return

        # now write data to bucket
        try:
            # [apr.20] WritePrecision.MN does not yet exist :|
            self._writeAPI.write( bucket=_bucket, record=df,
                                  data_frame_measurement_name=_measurement,
                                  data_frame_tag_columns=tags,
                                  write_precision=__class__.INFLUX_SENSORS_PRECISION )

        except Exception as ex:
            log.warning("exception detected while inserting measure: " + str(ex) )
            raise

        # tada ! :)
        return True



    # -----------------------------------------------------------
    # PROPERTIES
    #

    # sensors bucket name (read-only)
    @property
    def sensors_bucket(self):
        ''' read name of sensors' bucket '''
        return self._bucket_sensors



    # -----------------------------------------------------------
    # low-level methods
    # - _connect()              connect to influxDB
    # - _extract_uri()
    # - _extract_uri_metrics()  extract features to build a sensors' URI
    # - _extract_uri_sensors()  extract features to build a metrics' URI
    # - _buildQuery()           create Flux Query
    # - _is_duplicate()         check for already existing point
    #


    def _connect( self ):
        ''' get connected to the database '''

        if( self._client is not None ):
            log.warning("_client is Not None ?!?! ... continuing ...")
            time.sleep(2)
        try:
            # instantiate client & APIs
            url = "http://%s:%d" % ( str(self._influx_server), int(self._influx_port) )
            self._client = InfluxDBClient(url=url, token=self._influx_token, org=self._influx_org)
            self._writeAPI  = self._client.write_api(write_options=SYNCHRONOUS)
            self._queryAPI  = self._client.query_api()
            self._deleteAPI = self._client.delete_api()
        except Exception as ex:
            log.error("unable to connect to database '%s': " % (url) + str(ex))
            self._client = None


    def _extract_uri( self, topic, payload ):
        ''' extract features from topic and payload as a dictionnary for:
              1. sensor's URI
              2. metrics's URI
            (1) sensors URI --> <location,building,room,kind,unitID,subID>
                Note: if you have two neOSensors outside, they will
                    publish with same topics ... but will feature a
                    different unitID because of their mac address.
            (2) metrics' URI --> <direction,kind,avg>
                Note: direction is [in | out], kind is [load | bytes] and avg is [1mn | 5mn | 15mn]
         '''

        if( topic.startswith('$SYS') ):
            # metrics extraction
            return self._extract_uri_metrics( topic, payload)
        elif( self._bucket_sensors is not None ):
            # sensors extraction
            return self._extract_uri_sensors( topic, payload)
        else:
            raise Exception("don't know whether 'sensors' or 'metrics' uri to extract ?!?!")


    def _extract_uri_metrics( self, topic, payload ):
        ''' extract features from topic and payload as a dictionnary for
            metrics' URI --> <direction,kind,avg>
                Note: direction is [in | out], kind is [messages | bytes] and avg is [1mn | 5mn | 15mn]
        '''
        _topic_items = topic.split('/')

        # topics we'll process are:
        #   $SYS/broker/load/publish/received/{15min|5min|1min}
        #   $SYS/broker/load/publish/sent/{15min|5min|1min}
        #   $SYS/broker/load/bytes/received/{15min|5min|1min}
        #   $SYS/broker/load/bytes/sent/{15min|5min|1min}

        res = dict()

        # direction
        _direction = None
        if( _topic_items[-2].lower() == "received" ):
            _direction = 'in'
        elif( _topic_items[-2].lower() == "sent" ):
            _direction = 'out'
        else:
            raise Exception("topic '%s' does not feature a direction ?!?!" % str(topic))
        res['direction'] = _direction.lower()

        # kind
        _kind = None
        if( _topic_items[3].lower() == "publish" ):
            _kind = 'messages'
        elif( _topic_items[3].lower() == "bytes" ):
            _kind = 'bytes'
        else:
            raise Exception("topic '%s' does not feature a kindness ?!?!" % str(topic))
        res['kind'] = _kind.lower()

        # avg
        _avg = None
        if( _topic_items[-1].lower() == "15min" ):
            _avg = "15mn"
        elif( _topic_items[-1].lower() == "5min" ):
            _avg = "5mn"
        elif( _topic_items[-1].lower() == "1min" ):
            _avg = "1mn"
        else:
            raise Exception("topic '%s' does not feature an avg ?!?!" % str(topic))
        res['avg'] = _avg.lower()

        return res


    def _extract_uri_sensors( self, topic, payload ):
        ''' extract features from topic and payload as a dictionnary for
            sensors URI --> <location,building,room,kind,unitID,subID>
        '''
        _topic_items = topic.split('/')

        _abroad = False     # flag to know whether topic is of abroad type
        if( _topic_items[0].lower() == 'abroad' ):
            _abroad = True

        _outside = False    # flag to know whether we're outside or not
        if( _topic_items[0].lower() == 'outside' or
            payload.get('unitID') == 'outside' or 
            payload.get('subID') == 'outside' ):
            _outside = True

        res = dict()

        # location
        _location = payload.get('location')
        if( _location is None ):
            # location in topic ?
            if( _abroad is True ):
                _location = _topic_items[1]
            else:
                _location = __class__.DEFAULT_LOCATION
        res['location'] = _location.lower()

        # building, room
        if( _outside is True ):
            # outiside mean out-of a building but still within the UT3 campus
            # [apr.20] only managing outside/weather/<rain|wind|...>
            _building = None
            _room = None
        elif( _abroad is True ):
            # abroad/<location>/<building>/<room>/<kind>
            _building = _topic_items[2]
            _room = _topic_items[3]
        else:
            # general cas for UT3 end-devices <building>/<room>/<kind>
            _building = _topic_items[0]
            _room = _topic_items[1]
        res['building'] = _building.lower() if _building is not None else None
        res['room'] = _room.lower() if _room is not None else None

        # kind
        if( _abroad is True ):
            # abroad/carcassonne/outside/ambient/rain
            # abroad/carcassonne/home/kitchen/humidity
            _kind = _topic_items[4]
        else:
            # u4/302/temperature
            # outside/ambient/rain
            _kind = _topic_items[2]
        res['kind'] = _kind.lower()

        # unitID
        if( payload.get('unitID') is None or payload.get('unitID') == 'outside' ):
            # UT3 deprecated case
            # we need sensors unicity --> compose a unitID from '<building>_<room>'
            _unitID = _topic_items[0] + '_' + _topic_items[1]
        else:
            _unitID = payload.get('unitID')
        res['unitID'] = str(_unitID).lower() if _unitID is not None else None

        # subID
        if( payload.get('subID') is None or payload.get('subID') == 'outside' ):
            _subID = None
        else:
            _subID = payload.get('subID')
        res['subID'] = str(_subID).lower() if _subID is not None else None

        return res


    ''' return a Flux query '''
    def _buildQuery( self, bucket, measurement, startTime, endTime, tags, fields=None ):
        ''' this function will create a Flux query according to the input parameters
            Filter for non existing tags ought to get specified as None:
                e.g myTags['building'] = None
        '''

        # RFC-3339 --> 2019-08-21T06:00:00Z
        # ... but isoformat missing 'Z' ought to get added
        _strStartTime = startTime.replace(microsecond=0).isoformat()
        if( not _strStartTime.endswith('Z') ):
            _strStartTime += 'Z'
        _strEndTime = endTime.replace(microsecond=0).isoformat()
        if( not _strEndTime.endswith('Z') ):
            _strEndTime += 'Z'

        _query = f'from(bucket: "{bucket}") \
|> range(start: {_strStartTime}, stop: {_strEndTime}) \
|> filter(fn: (r) => r._measurement == "{measurement}")'
        
        if( tags is not None and len(tags) ):
            _query += ' |> filter(fn: (r) => '
            for _tagKey,_tagValue in tags.items():
                if( _tagValue is not None ):
                    _query += f'r.{_tagKey} == "{_tagValue}" and '
                else:
                    _query += f'not exists r.{_tagKey} and '
            _query = _query.rstrip().rsplit(' ',1)[0]
            _query += ')'

        # optional fields
        if( fields is not None and len(fields) ):
            _query += ' |> filter(fn: (r) => '
            for _curField in fields:
                _query += f'r._field == "{_curField}" or '
            _query = _query.rstrip().rsplit(' ',1)[0]
            _query += ')'

        return _query


    ''' function to check whether a duplicate point already exists '''
    def _is_duplicate( self, point=None, interval=None ):
        ''' check for duplicate entry in influxDB vs record.
            An optional interval may express the amount of seconds before AND after
            the record's timestamp to look for
        '''
        if( point is None): return False

        # timestamp interval variation
        if( interval is not None ):
            _offset = abs(int(interval))
        else:
            if( __class__.INFLUX_SENSORS_PRECISION == WritePrecision.S ):
                _offset=1
            else:
                # all others resolution are below 'second'
                _offset=1

        # time boundaries
        _startTime = point._time.replace(microsecond=0) - timedelta(seconds=_offset)
        _endTime = point._time.replace(microsecond=0) + timedelta(seconds=_offset)

        '''        
        # TESTS TESTS TESTS
        _endTime = datetime.now(timezone.utc)
        ##_strEndTime = _endTime.isoformat()
        _startTime = _endTime - timedelta(minutes=5)
        ##_strStartTime = _startTime.isoformat()
        #_query = f'from(bucket: "{self._bucket_sensors}") |> range(start: {_strStartTime}, stop: {_strEndTime}) |> filter(fn: (r) => r["_measurement"] == "{__class__.MEASUREMENT_SENSORS}")'
        #_query = f'from(bucket: "{self._bucket_sensors}") |> range(start: -5m) |> filter(fn: (r) => r["_measurement"] == "{__class__.MEASUREMENT_SENSORS}")' [ok]
        #_query = f'from(bucket: "sensors") |> range(start: -5m) |> filter(fn: (r) => r["_measurement"] == "data")' [ok]

        _query = f'from(bucket: "{self._bucket_sensors}") \
|> range(start: {_strStartTime}, stop: {_strEndTime}) \
|> filter(fn: (r) => r._measurement == "{__class__.MEASUREMENT_SENSORS}")'
        #_query += ' |> filter(fn: (r) => r.building == "bu") '
        '''        

        # retrieve Flux query
        _query = self._buildQuery( bucket=self._bucket_sensors,
                                    measurement=__class__.MEASUREMENT_SENSORS,
                                    startTime=_startTime,
                                    endTime=_endTime,
                                    tags=point._tags,
                                    fields=['value'] )

        if getLogLevel().lower() == "debug":
            print(_query)

        # execute query ...
        _records = self._queryApi.query_stream( _query )

        # ... is there any matching record ?
        try:
            rec = next(_records)
            if( rec is not None ):
                #print(rec)
                #log.debug("at least one DUPLICATE ...")
                return True
        except Exception as ex:
            # just mean there are no duplicate ;)
            return False

        return False    # just in case ;)

