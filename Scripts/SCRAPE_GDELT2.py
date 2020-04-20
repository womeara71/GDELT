#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 11:35:17 2019

@author: jamesdawahare
"""

import urllib.request
import datetime


days_to_collect = 28
#days_to_collect = 1
GKG = False
Mentions = False
Events = True


start = datetime.datetime(year=2019,month=6,day=22,hour=23,minute=45)
date_list = [start - datetime.timedelta(minutes=15*x) for x in range(days_to_collect*96)]
date_list = list(map(lambda x: x.strftime("%Y%m%d%H%M") + '00', date_list))
                    
for date in date_list: 
    print("Doing: {}".format(date))
    if GKG == True:
        url = 'http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip'.format(date)
        try:
            urllib.request.urlretrieve(url,'C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Data/zips/GDELT2/{}.gkg.csv.zip'.format(date))
        except:
            print(date)
            
    if Mentions == True:
        url = 'http://data.gdeltproject.org/gdeltv2/{}.mentions.CSV.zip'.format(date)
        try:
            urllib.request.urlretrieve(url,'C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Data/zips/GDELT2/{}.mentions.CSV.zip'.format(date))
        except:
            print(date)
    
    if Events == True:
        url = 'http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip'.format(date)
        try:
            urllib.request.urlretrieve(url,'C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Data/zips/GDELT2/{}.events.CSV.zip'.format(date))
        except:
            print(date)