#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 11:35:17 2019

@author: jamesdawahare
"""

import urllib.request
from os import path
import time



url = 'http://data.gdeltproject.org/events/20130401.export.CSV.zip'

#############before running again add timer to wait and check if file already downloaded


for year in range(2013,2016+1):
    year=str(year)
    for month in range(1,12+1):
        month = str(month).zfill(2)
        for day in range(1,31+1):
            day = str(day).zfill(2)
            url = 'http://data.gdeltproject.org/events/{}.export.CSV.zip'.format(year+month+day)
            if path.exists('Data_GDELT/{}.export.CSV.zip'.format(year+month+day)):
                continue
            try:
                urllib.request.urlretrieve(url,'Data_GDELT/{}.export.CSV.zip'.format(year+month+day))
            except:
                print(year+month+day)
                
            
                
            


#urllib.request.urlretrieve(url, 'Data_GDELT/{}'.format('20130401.export.CSV.zip'))