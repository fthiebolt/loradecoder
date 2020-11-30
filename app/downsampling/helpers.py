#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# ihelper module for sensors data aggregation (i.e downsampling)
#
# This module holds various helpers function used during sensor's data aggregation
# _____________________________________________________________________________
# TODO:
# _____________________________________________________________________________
#
# F.Thiebolt    jun.20  initial release
#



# #############################################################################
#
# Import zone
#
import os
import sys
import math
import signal
import time
import json
from datetime import datetime, timedelta


# pandas, numpy
import pandas as pd
import numpy as np

# --- project related imports
from logger.logger import log, getLogLevel
import settings



# #############################################################################
#
# Functions
#

#
# Function for pretty printing aggregated data
def pprint_aggregation( df, _time=None ):

    # simulate UTC for fixed _time
    row_time = _time
    if( isinstance(_time,datetime) ):
        row_time = _time.isoformat()
    elif( isinstance(_time,str) ):
        row_time = _time

    if( row_time is not None and row_time.endswith('Z') is not True ):
        row_time += 'Z'

    # list of columns
    df_columns = list(df.columns)

    # parse all DF rows ...
    for index, row in df.iterrows():

        # time
        if( _time is None ):
            row_time = index

        # sensor identity
        uri = ""
        for col in settings.INFLUX_SENSORS_BUCKET_TAGS:
            if( col in df_columns ):
                uri += df[col].iloc[0]+'/'
                continue
            uri += '__/'
        uri = uri.rstrip().rsplit('/',1)[0]

        # values
        value_min   = row['value_min']
        value_avg   = row['value_avg']
        value_max   = row['value_max']
        value_units = row['value_units']

        # _time is 20 bytes length
        if( isinstance(row['value_min'],str) ):
            # complex values
            value_min   = json.loads(value_min)
            value_avg   = json.loads(value_avg)
            value_max   = json.loads(value_max)
            value_units = json.loads(value_units)

            print(f"{row_time}  {uri:>56}  {value_units}")
            print(f"{' ':80}{value_min}")
            print(f"{' ':80}{value_avg}")
            print(f"{' ':80}{value_max}")
        else:
            print(f"{row_time}  {uri:>56}  {value_min:>8.2f}  {value_avg:>8.2f}  {value_max:>8.2f}  {value_units}")



#
# Function to compute Wind minimum
def windMin( df ):
    '''
df:
   windDir  windSpeed_kph  windGust_kph  windGustDir
0   147.13           4.10         14.48        157.5
1   162.44           5.72         11.27        180.0
2   154.61           7.34         14.48        157.5
    '''

    df_res = pd.DataFrame( columns=df.columns.tolist() )

    # min for wind & speed
    _idx = df['windSpeed_kph'].idxmin()
    df_res.loc[0,'windDir'] = df.loc[_idx,'windDir']
    df_res.loc[0,'windSpeed_kph'] = df.loc[_idx,'windSpeed_kph']

    # min for gust wind & speed
    _idx = df['windGust_kph'].idxmin()
    df_res.loc[0,'windGustDir'] = df.loc[_idx,'windGustDir']
    df_res.loc[0,'windGust_kph'] = df.loc[_idx,'windGust_kph']

    return df_res



#
# Function to compute Wind maximum
def windMax( df ):
    '''
df:
   windDir  windSpeed_kph  windGust_kph  windGustDir
0   147.13           4.10         14.48        157.5
1   162.44           5.72         11.27        180.0
2   154.61           7.34         14.48        157.5
    '''

    df_res = pd.DataFrame( columns=df.columns )

    # max for wind & speed
    _idx = df['windSpeed_kph'].idxmax()
    df_res.loc[0,'windDir'] = df.loc[_idx,'windDir']
    df_res.loc[0,'windSpeed_kph'] = df.loc[_idx,'windSpeed_kph']

    # max for gust wind & speed
    _idx = df['windGust_kph'].idxmax()
    df_res.loc[0,'windGustDir'] = df.loc[_idx,'windGustDir']
    df_res.loc[0,'windGust_kph'] = df.loc[_idx,'windGust_kph']

    return df_res



#
# Function to compute Wind average
def windAvg( df ):
    '''
df:
   windDir  windSpeed_kph  windGust_kph  windGustDir
0   147.13           4.10         14.48        157.5
1   162.44           5.72         11.27        180.0
2   154.61           7.34         14.48        157.5
    '''

    df_res = pd.DataFrame( columns=df.columns )

    # avg on wind & speed
    _excerpt = df.loc[:,['windDir','windSpeed_kph']].copy()
    _excerpt.rename(columns={'windDir':'dir','windSpeed_kph':'speed'}, inplace=True )
    windSpeed, windDir = windvec( _excerpt )
    windSpeed = round(windSpeed,settings.AGG_DATA_PRECISION)
    df_res.loc[0, ('windDir','windSpeed_kph')] = windDir, windSpeed

    # avg on gust wind & speed
    _excerpt = df.loc[:,['windGustDir','windGust_kph']].copy()
    _excerpt.rename(columns={'windGustDir':'dir','windGust_kph':'speed'}, inplace=True )
    windSpeed, windDir = windvec( _excerpt )
    df_res.loc[0, ('windGust_kph','windGustDir')] = windSpeed, windDir

    # round elementwise
    df_res = df_res.applymap(lambda x: round(x,settings.AGG_DATA_PRECISION))

    return df_res



#
# Function to compute aggregated Wind average
# Note: this function takes advantage of the _avg_count counter
def aggWindAvg( df ):
    '''
average on aggregated data, df:
   windDir  windSpeed_kph  windGust_kph  windGustDir  _avg_count
0      0.0            0.0           0.0          0.0         1.0
1      0.0            0.0           0.0          0.0         NaN
    '''
    df_res = pd.DataFrame( columns=df.columns ).drop(['_avg_count'], axis=1, errors='ignore')

    # avg on wind & speed
    _excerpt = df.loc[:,['windDir','windSpeed_kph','_avg_count']].copy()
    _excerpt.rename(columns={'windDir':'dir','windSpeed_kph':'speed','_avg_count':'factor'}, inplace=True )
    windSpeed, windDir = windvec( _excerpt )
    df_res.loc[0, ('windDir','windSpeed_kph')] = windDir, windSpeed

    # avg on gust wind & speed
    _excerpt = df.loc[:,['windGustDir','windGust_kph','_avg_count']].copy()
    _excerpt.rename(columns={'windGustDir':'dir','windGust_kph':'speed','_avg_count':'factor'}, inplace=True )
    windSpeed, windDir = windvec( _excerpt )
    df_res.loc[0, ('windGust_kph','windGustDir')] = windSpeed, windDir

    # round elementwise
    df_res = df_res.applymap(lambda x: round(x,settings.AGG_DATA_PRECISION))

    return df_res



#
# Function to compute average of wind vectors
def windvec( df ):
    '''
average on aggregated data, df:
   dir  speed  factor
0  12.0   4.2     5.0
1  13.8   2.3     NaN

average on regular data, df:
    dir     speed
0   147.13   4.10
1   162.44   5.72
2   154.61   7.34
    '''

    ve = 0.0 # define east component of wind speed
    vn = 0.0 # define north component of wind speed
    count = 0.0

    for _, row in df.iterrows():
        if( pd.isna(row['dir']) is True or pd.isna(row['speed']) is True ):
            continue

        if( 'factor' in df.columns and pd.isna(row['factor']) is not True ):
            factor = row['factor']
        else:
            factor = 1.0
        count += factor

        if( row['speed'] == 0.0 ):
            continue

        ve += row['speed'] * math.sin(row['dir']*math.pi/180.0)
        vn += row['speed'] * math.cos(row['dir']*math.pi/180.0)

    if( count == 0 ):
        return None, None

    ve = - ve / count # determine average east speed component
    vn = - vn / count # determine average north speed component

    uv = math.sqrt(ve * ve + vn * vn) # calculate wind speed vector magnitude
    # Calculate wind speed vector direction
    vdir = np.arctan2(ve, vn)
    vdir = vdir * 180.0 / math.pi # Convert radians to degrees

    if vdir < 180:
        Dv = vdir + 180.0
    else:
        if vdir > 180.0:
            Dv = vdir - 180
        else:
            Dv = vdir

    return uv, Dv # uv is speed in same units as input, Dv in degrees from North


