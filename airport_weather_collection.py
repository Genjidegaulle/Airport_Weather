#!/usr/bin/env python
# coding: utf-8

# ### Additional Weather Data Collection ###
# 
# A huge thanks to [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/request/download.phtml?network=IL_ASOS#), especially Daryl Herzmann, for providing a solid archive and [script](https://github.com/akrherz/iem/blob/master/scripts/asos/iem_scraper_example.py) to obtain the data.
# 
# Approximate Runtime: 10 minutes

# In[1]:


"""
Example script that scrapes data from the IEM ASOS download service
"""
from __future__ import print_function
import json
import time
import datetime
import pandas as pd
import numpy as np
# Python 2 and 3: alternative 4
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


# In[2]:


def download_data(uri):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode('utf-8')
            if data is not None and not data.startswith('ERROR'):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_stations_from_filelist(filename):
    """Build a listing of stations from a simple file listing the stations.
    The file should simply have one station per line.
    """
    stations = []
    for line in open(filename):
        stations.append(line.strip())
    return stations


# In[3]:


# timestamps in UTC to request data for
startts = datetime.datetime(2017, 1, 1)
endts = datetime.datetime(2019, 8, 27)

service = SERVICE + "&data=tmpf&data=dwpf&data=relh&data=feel&data=drct&data=sped&data=alti&data=mslp&data=p01i&data=vsby&data=gust_mph&" +                     "tz=Etc%2FUTC&format=onlycomma&latlon=no&missing=null&trace=null&direct=no&report_type=2&"

service += startts.strftime('year1=%Y&month1=%m&day1=%d&')
service += endts.strftime('year2=%Y&month2=%m&day2=%d&')

# Two examples of how to specify a list of stations
#stations = get_stations_from_filelist("airport_list.txt")
stations = ['CAK', 'AKC', 'ALB', 'ABQ', 'ANC', 'ATW', 'ATL', 'ACY', 'AUS', 'BWI', 'BGR', 'BHM', 'BOI', 'BOS', 'BUF', 'CLT',             'CHS', 'MDW', 'ORD', 'CVG', 'CLE', 'CMH', 'LCK', 'DFW', 'DAY', 'DEN', 'DSM', 'DTW', 'FAI', 'FLL', 'RSW', 'FAT',             'BDL', 'GRB', 'GSO', 'ITO', 'HNL', 'IAH', 'HOU', 'HSV', 'IND', 'JAX', 'JNU', 'MCI', 'KTN', 'EYW', 'KOA', 'TYS',             'LAL', 'LAN', 'LAS', 'LIT', 'LAX', 'SDF', 'MLB', 'MEM', 'MIA', 'MAF', 'MKE', 'MSP', 'MYR', 'BNA', 'MSY', 'JFK',             'LGA', 'EWR', 'SWF', 'ORF', 'OAK', 'OKC', 'OMA', 'ONT', 'SNA', 'MCO', 'SFB', 'PSP', 'ECP', 'PNS', 'PHL', 'PHX',             'AZA', 'PIT', 'PWM', 'PDX', 'PVD', 'RDU', 'RNO', 'RIC', 'RST', 'ROC', 'RFD', 'SMF', 'SLC', 'SAT', 'SBD', 'SAN',             'SFO', 'SJC', 'SRQ', 'SAV', 'LKE', 'BFI', 'SEA', 'PAE', 'GEG', 'STL', 'PIE', 'SYR', 'TLH', 'TPA', 'DCA', 'IAD',             'PBI', 'AVP', 'ILM']
stations[0] = ''.join([c for c in stations[0] if ord(c) < 128])
stations.sort()

weather_df = pd.DataFrame()
for station in stations:
    uri = '%s&station=%s' % (service, station)
    print('Downloading: %s' % (station, ))
    data = download_data(uri).split('\n')
    data[:] = [list(d.split(',')) for d in data]
    if len(data) > 2:
        weather_df = weather_df.append(pd.DataFrame(data[1:], columns=data[0]))

# Uncomment the code below if you'd like to store the raw data within a file
#     out = open('weather_full_unaggregated.csv', 'a')
#     out.write(data)
#     out.close()
    print('Finished: %s' % (station, ))


# In[4]:


# If you saved the data within a csv, load it in using this line
# weather = pd.read_csv('weather_full_unaggregated.csv')

# Otherwise:
weather = weather_df[weather_df.station != 'station']
weather.dropna(how='any', inplace=True)
weather


# In[5]:


weather = weather.rename(columns = {'station':'Airport', 'valid':'Date', 'tmpf':'Temperature', 'feel': 'Apparent Temperature',                           'dwpf':'Dew Point Temp', 'relh':'Relative Humidity %', 'drct':'Wind Direction (degrees from N)',                           'sped':'Wind Speed', 'p01i':'One Hour Precipitation', 'alti':'Pressure Altimeter', 'mslp':'Sea Level Pressure',                           'vsby':'Visibility', 'gust_mph':'Gust'})
weather['Date'] = weather['Date'].astype('datetime64[ns]')
weather.head()


# In[6]:


weather_a = weather[['Temperature', 'Dew Point Temp', 'Relative Humidity %', 'One Hour Precipitation',                               'Wind Speed', 'Pressure Altimeter', 'Sea Level Pressure']]
weather_a = weather_a.apply(pd.to_numeric, errors='coerce')
weather_a.insert(0, 'Airport', weather['Airport'])
weather_a.insert(1, 'Date', weather['Date'])


# In[7]:


airports = stations
def aggregation(weather):

    weather_aggregated = pd.DataFrame()
    
    # Aggregate each airport by day
    for a in airports:
        each_airport = weather[weather['Airport'] == a]
        each_airport = each_airport.groupby([each_airport['Date'].dt.date]).mean().round(2)
        each_airport.insert(0, 'Airport', a)
        weather_aggregated = pd.concat([weather_aggregated, each_airport])
        
    return weather_aggregated


# In[8]:


weather_aggregated = aggregation(weather_a)
weather_aggregated = weather_aggregated.rename(columns={'One Hour Precipitation': 'Average Precipitation'})
weather_aggregated.head()


# In[9]:


weather_aggregated.to_csv('weather_full_aggregated.csv')


# In[ ]:




