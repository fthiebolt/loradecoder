#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# sensor data aggregation (i.e downsampling) module featuring
#   additional aggregation related operations
#
# This module compute min,avg and max from a single sensor's data.
# _____________________________________________________________________________
# TODO:
# _____________________________________________________________________________
#
# F.Thiebolt    jun.20  shift to high and low resolution buckets
# F.Thiebolt    may.20  initial release
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
from datetime import datetime,timedelta


# pandas, numpy
import pandas as pd
import numpy as np

# --- project related imports
from logger.logger import log, getLogLevel
import settings

from .helpers import windMin, windAvg, windMax, aggWindAvg



# #############################################################################
#
# Functions
#

#
# Function to retrieve sensors data in a DF for further aggregation
def getSensorsData( db, aggDate, interval_mn, *args, **kwargs ):
    ''' aggDate [DATETIME format] is the end of the interval e.g 13:05:00,
        hence we'll retrieve data from start_date = 13:00:01
    '''

    _params = dict()

    # start date
    if( 'start_date' in kwargs.keys() ):
        _params['start_date'] = kwargs.get('start_date').replace(microsecond=0)
    else:
        _start_date = aggDate.replace(second=0,microsecond=0) - timedelta(seconds=interval_mn*60 - 1)
        _start_date = _start_date.isoformat()
        _params['start_date'] = _start_date

    # end date
    if( 'end_date' in kwargs.keys() ):
        _params['end_date'] = kwargs.get('end_date').replace(microsecond=0)
    else:
        _stop_date = aggDate.replace(second=0,microsecond=0)
        _stop_date = _stop_date.isoformat()
        _params['end_date'] = _stop_date

    # tags
    _params['tags'] = kwargs.get('tags')

    # fields
    _params['fields'] = ["value","value_units"]

    # query addon
    _myQueryAddon =' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    _tags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_TAGS)
    _flags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_FLAGS)
    _myQueryAddon += f' |> keep(columns: [ {_tags2keep},{_flags2keep} ])'
    _params['query_addon'] = _myQueryAddon

    #print(f"parameters:\n{_params}")
    #sys.exit(1)

    return db.get_df( **_params )



#
# Function to aggregate data from a SINGLE sensor
def sensor_aggregator( group, *args, **kwargs ):
    ''' this function process a group of measure from a SINGLE sensor.
        return a single row df with additionnal columns:
            value_min, value_avg, value_max (instead of 'value')
    '''

    #print(f">>> IN:\n{group}\n---")
    if( group['kind'].iloc[0].lower()=='shutter' ):

        log.debug("shutter are NOT yet processed ...")
        return None

    elif( isinstance(group['value'].iloc[0],str) ):
        # processing json complex data (e.g energy, ambiant, wind ...)
        log.debug("complex values detected ... ")

        # recreate column oriented data ...
        _data = pd.DataFrame()
        for _, _row in group.iterrows():

            _idf = pd.DataFrame([json.loads(_row['value'])], columns=json.loads(_row['value_units']))
            _data = _data.append( _idf, ignore_index=True )
        #print(f"data_shape: {_data.shape}:\n{_data}")

        # processing ...
        if( group['kind'].iloc[0].lower()=='wind' ):
            # [complex] WIND data
            '''
_data:
   windDir  windSpeed_kph  windGust_kph  windGustDir
0   147.13           4.10         14.48        157.5
1   162.44           5.72         11.27        180.0
2   154.61           7.34         14.48        157.5
            '''

            # ... generate row
            res = group.drop(['result','table','value'],axis=1,errors='ignore').head(1)
            # COMPUTE MIN
            res['value_min'] = json.dumps( windMin(_data).iloc[0].values.tolist())
            # COMPUTE MAX
            res['value_max'] = json.dumps( windMax(_data).iloc[0].values.tolist())
            # COMPUTE AVG
            res['value_avg'] = json.dumps( windAvg(_data).iloc[0].values.tolist())

        else:

            # [complex]: others data
            _df = _data.agg(['min', 'mean', 'max'])
            _df = _df.applymap(lambda x: round(x,settings.AGG_DATA_PRECISION))
            #print(_df)
            # ... generate row
            res = group.drop(['result','table','value'],axis=1,errors='ignore').head(1)
            res['value_min'] = json.dumps(_df.loc['min'].values.tolist())
            res['value_avg'] = json.dumps(_df.loc['mean'].values.tolist())
            res['value_max'] = json.dumps(_df.loc['max'].values.tolist())

    else:
        # processing regular data ...
        _df = group.agg({'value':['min','mean','max']})
        _df['value'] = _df['value'].apply(lambda x: round(x,settings.AGG_DATA_PRECISION))
        #print(f"regular agg:\n{_df}")
        # ... generate row
        res = group.drop(['result','table','value'],axis=1,errors='ignore').head(1)
        res['value_min'] = _df['value'].loc['min']
        res['value_avg'] = _df['value'].loc['mean']
        res['value_max'] = _df['value'].loc['max']

    # return
    #print(f"{res}\n<<<")
    return res



#
# Function to save [INTERVAL] mn aggregated data
def writeAgg( db, df, aggDate ):
    ''' db: influxDB client
        df: aggregated data in a DF to get written
        aggDate: [DATETIME format]
    '''

    # in hires bucket interval ?
    diff = datetime.utcnow() - aggDate
    if( diff.total_seconds() > 7*24*3600 ):
        log.debug(f"aggregation date {aggDate} > 7 days old ... out-of-range for sensors_hires bucket")
        return None

    # set UTC zone
    if( aggDate.isoformat().endswith('Z') is not True ):
        df['_time'] = aggDate.isoformat() + 'Z'
    else:
        df['_time'] = aggDate.isoformat()
    df.set_index('_time', inplace=True)

    # store aggregation to 'sensors_hires'
    params = dict()
    params['bucket'] = db.sensors_bucket + '_hires'
    params['tags'] = list( set(settings.INFLUX_SENSORS_BUCKET_TAGS) & set(list(df.columns)) )

    db.write_df( df, **params )



#
# Function to update DAY in lowres aggregation bucket
def updateAggDay( db, df, aggDate ):
    ''' aggDate [DATETIME format] is the end of the interval e.g 13:05:00
        db is the influxDB client
        df is a single row pandas DataFrame with INTERVALmn aggregation
            WARNING: df['_time'] OUGHT to get changed
        Fields:['value_min','value_avg','value_max','_avg_count']
    '''

    # GET DF day from sensors_lowres
    # start_date = aggDate with hour,min,sec,µsec=0
    # interval = 0 (end_date is the same as start_date
    _params = dict()

    _start_date = aggDate.replace(hour=0,minute=0,second=0,microsecond=0)
    _start_date = _start_date.isoformat()
    _params['start_date'] = _start_date

    _params['interval'] = 1     # empty range is not allowed

    # send all field as default
    #_params['fields'] = ["value_min","value_avg","value_max","_avg_count","value_units"]

    _tags = dict()
    for tagName in settings.INFLUX_SENSORS_BUCKET_TAGS:
        if( tagName in list(df.columns) ):
            # TAG exist and has a value
            _tags[ tagName ] = df[ tagName ].iloc[0]
        else:
            # TAG does not exist
            _tags[ tagName ] = None
    _params['tags'] = _tags

    _params['bucket'] = db.sensors_bucket + "_lowres"


    _myQueryAddon =' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    _tags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_TAGS)
    _flags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_FLAGS)
    _myQueryAddon += f' |> keep(columns: [ {_tags2keep},{_flags2keep} ])'
    _params['query_addon'] = _myQueryAddon

    df_day = db.get_df( **_params )


    # check results
    if( df_day.shape[0] > 1 ):
        print(df_day)
        raise Exception("ouch, we're supposed to have a single row of data")

    # compute min / avg / max
    df_day = aggMerge( df_day, df ).drop(['result','table'],axis=1,errors='ignore')

    # write updated DAY to sensors_lowres
    writeAggDay( db, df_day, aggDate.replace(hour=0,minute=0,second=0,microsecond=0) )



#
# Function to save DAY aggregation in lowres sensors bucket
def writeAggDay( db, df, aggDate ):
    ''' db: influxDB client
        df: aggregated data in a DF to get written
        aggDate: [DATETIME format]
    '''

    # in lowres bucket interval ?
    diff = datetime.utcnow() - aggDate
    if( diff.total_seconds() > 365*24*3600 ):
        log.debug(f"aggregation date {aggDate} > 1 year old ... out-of-range for sensors_lowres bucket")
        return None

    # set UTC zone
    if( aggDate.isoformat().endswith('Z') is not True ):
        df['_time'] = aggDate.isoformat() + 'Z'
    else:
        df['_time'] = aggDate.isoformat()
    df.set_index('_time', inplace=True)

    # store aggregation to 'sensors_month'
    params = dict()
    params['bucket'] = db.sensors_bucket + '_lowres'
    params['tags'] = list( set(settings.INFLUX_SENSORS_BUCKET_TAGS) & set(list(df.columns)) )

    db.write_df( df, **params )



#
# Function to update MONTH in year aggregation
#
# [jun.20] DEPRECATED since we're using lowres with 1day resolution across 1 year
#
def updateAggMonth( db, df, aggDate ):
    ''' aggDate is the end of the interval e.g 13:05:00
        db is the influxDB client
        df is a single row pandas DataFrame with INTERVALmn aggregation
            WARNING: df['_time'] OUGHT to get changed
        Fields:['value_min','value_avg','value_max','_avg_count']
    '''

    # GET DF month from sensors_year
    # start_date = aggDate with day=1,hour,min,sec,µsec=0
    # interval = 0 (end_date is the same as start_date
    _params = dict()

    _start_date = aggDate.replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    _start_date = _start_date.isoformat()
    _params['start_date'] = _start_date

    _params['interval'] = 1     # empty range is not allowed

    # send all field as default
    #_params['fields'] = ["value_min","value_avg","value_max","_avg_count","value_units"]

    _tags = dict()
    for tagName in settings.INFLUX_SENSORS_BUCKET_TAGS:
        if( tagName in list(df.columns) ):
            # TAG exist and has a value
            _tags[ tagName ] = df[ tagName ].iloc[0]
        else:
            # TAG does not exist
            _tags[ tagName ] = None
    _params['tags'] = _tags

    _params['bucket'] = db.sensors_bucket + "_year"


    _myQueryAddon =' |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
    _tags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_TAGS)
    _flags2keep = ",".join(f'"{item}"' for item in settings.INFLUX_SENSORS_BUCKET_FLAGS)
    _myQueryAddon += f' |> keep(columns: [ {_tags2keep},{_flags2keep} ])'
    _params['query_addon'] = _myQueryAddon

    df_month = db.get_df( **_params )

    # check results
    if( df_month.shape[0] > 1 ):
        print(df_month)
        raise Exception("ouch, we're supposed to have a single row of data")

    # compute min / avg / max
    df_month = aggMerge( df_month, df ).drop(['result','table'],axis=1,errors='ignore')

    # write updated MONTH to sensors_year
    writeAggMonth( db, df_month, aggDate.replace(day=1,hour=0,minute=0,second=0,microsecond=0) )



#
# Function to save MONTH aggregation to YEAR sensors bucket
#
# [jun.20] DEPRECATED since we're using lowres with 1day resolution across 1 year
#
def writeAggMonth( db, df, aggDate ):
    ''' db: influxDB client
        df: aggregated data in a DF to get written
        aggDate: [DATETIME format]
    '''

    # in YEAR bucket interval ?
    diff = datetime.utcnow() - aggDate
    if( diff.total_seconds() > 365*24*3600 ):
        log.debug(f"aggregation date {aggDate} > 1 year old ... out-of-range for sensors_year bucket")
        return None


    # set UTC zone
    if( aggDate.isoformat().endswith('Z') is not True ):
        df['_time'] = aggDate.isoformat() + 'Z'
    else:
        df['_time'] = aggDate.isoformat()
    df.set_index('_time', inplace=True)

    # store aggregation to 'sensors_month'
    params = dict()
    params['bucket'] = db.sensors_bucket + '_year'
    params['tags'] = list( set(settings.INFLUX_SENSORS_BUCKET_TAGS) & set(list(df.columns)) )

    db.write_df( df, **params )



#
# Function to merge two aggregated DF
def aggMerge( df_input, df_value ):

    if( df_input.shape[0] == 0 ):
        # first aggregation for sensor
        log.debug("first time AGG for sensor ...")
        df_res = df_value.copy()
        df_res['_avg_count'] = 1
        return df_res

    # MIN
    if( isinstance( df_value['value_min'].iloc[0], str) ):
        # processing json complex data (e.g energy, ambiant, wind ...)
        _dataList1 = json.loads( df_input['value_min'].iloc[0] )
        _dataList2 = json.loads( df_value['value_min'].iloc[0] )

        #df_input['value_min'].iloc[0] = json.dumps( [ min(a,b) for a,b in zip(_dataList1, _dataList2) ] )
        df_input.loc[0, 'value_min'] = json.dumps( [ min(a,b) for a,b in zip(_dataList1, _dataList2) ] )
    else:
        #df_input['value_min'].iloc[0] = min( df_input['value_min'].iloc[0], df_value['value_min'].iloc[0] )
        df_input.loc[0, 'value_min'] = min( df_input['value_min'].iloc[0], df_value['value_min'].iloc[0] )

    # MAX
    if( isinstance( df_value['value_max'].iloc[0], str) ):
        # processing json complex data (e.g energy, ambiant, wind ...)
        _dataList1 = json.loads( df_input['value_max'].iloc[0] )
        _dataList2 = json.loads( df_value['value_max'].iloc[0] )

        #df_input['value_max'].iloc[0] = json.dumps( [ max(a,b) for a,b in zip(_dataList1, _dataList2) ] )
        df_input.loc[0, 'value_max'] = json.dumps( [ max(a,b) for a,b in zip(_dataList1, _dataList2) ] )
    else:
        #df_input['value_max'].iloc[0] = max( df_input['value_max'].iloc[0], df_value['value_max'].iloc[0] )
        df_input.loc[0, 'value_max'] = max( df_input['value_max'].iloc[0], df_value['value_max'].iloc[0] )


    # AVG
    # Note: we'll make use of the '_avg_count field
    _avg_count = df_input['_avg_count'].iloc[0]
    if( isinstance( df_value['value_avg'].iloc[0], str) ):
        # processing json complex data (e.g energy, ambiant, wind ...)

        if( df_value['kind'].iloc[0].lower() == "wind" ):
            # [complex] WIND DATA

            #print(f"df_input:\n{df_input}\ndf_value:\n{df_value}")

            # create dataframe for value_avg data with value_units as columns
            # Note: this one feature _avg_count
            _df = pd.DataFrame( [ json.loads(df_input.loc[0,'value_avg']) ], columns=json.loads(df_input.loc[0,'value_units']) )
            _df['_avg_count'] = _avg_count

            _df = _df.append( pd.DataFrame( [ json.loads(df_value['value_avg'].iloc[0]) ], columns=json.loads(df_value['value_units'].iloc[0]) ), ignore_index=True )

            _df_res = aggWindAvg( _df )

            #df_input['value_avg'].iloc[0] = json.dumps( _df_res.iloc[0].values.tolist() )
            df_input.loc[0, 'value_avg'] = json.dumps( _df_res.iloc[0].values.tolist() )

        else:
            # [complex] others data
            _dataList1 = json.loads( df_input['value_avg'].iloc[0] )
            _dataList2 = json.loads( df_value['value_avg'].iloc[0] )
    
            #df_input['value_avg'].iloc[0] = json.dumps( [ (a*_avg_count+b)/(_avg_count+1) for a,b in zip(_dataList1, _dataList2) ] )
            df_input.loc[0, 'value_avg'] = json.dumps( [ (a*_avg_count+b)/(_avg_count+1) for a,b in zip(_dataList1, _dataList2) ] )

    else:
        # avg on regular data
        #df_input['value_avg'].iloc[0] = (df_input['value_avg'].iloc[0] * _avg_count + df_value['value_avg'].iloc[0]) / (_avg_count + 1)
        df_input.loc[0, 'value_avg'] = (df_input['value_avg'].iloc[0] * _avg_count + df_value['value_avg'].iloc[0]) / (_avg_count + 1)

    #df_input['_avg_count'].iloc[0] = _avg_count + 1
    df_input.loc[0, '_avg_count'] = _avg_count + 1

    return df_input



#
# Function to retrieve latest indexation from INTERVALmn aggregation bucket (i.e sensors_hires)
def getAggLastIndex( db ):

    # GET latest DF from sensors_hires
    _params = dict()

    # end date
    _params['end_date'] = datetime.utcnow().replace( microsecond=0 )

    # start date
    _params['start_date'] = _params['end_date'] - timedelta( weeks=1 )

    # tags
    _params['tags'] = None

    # fields
    _params['fields'] = [ 'value_avg' ]

    # bucket
    _params['bucket'] = db.sensors_bucket + "_hires"

    # query addon
    _myQueryAddon = '|> keep(columns: ["_time"]) \
|> max(column: "_time")'
    _params['query_addon'] = _myQueryAddon

    _records = db.find( **_params )

    # check results
    if( _records is None ): return None

    # extract results from iterator
    try:
        rec = next(_records)
        if( rec is not None ):
            '''
            print(f"first extracted record:\n{rec}")
FluxRecord() table: 0, {'result': '_result', 'table': 0, '_time': datetime.datetime(2020, 6, 14, 15, 5, tzinfo=datetime.timezone.utc)}
            print(f"time of record is:\n{rec['_time']}")
            '''
            return rec["_time"].replace( tzinfo=None )  # naive UTC
    except Exception as ex:
        # just mean there's nothing to parse
        return None
    
    return None

