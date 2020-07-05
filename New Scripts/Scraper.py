# -*- coding: utf-8 -*-
"""
Created on Tue Jun 30 20:13:49 2020

@author: 605453
"""

import urllib.request
import datetime


class Scraper(object):
    
    events = 'http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip'
    mentions = 'http://data.gdeltproject.org/gdeltv2/{}.mentions.csv.zip'
    gkg = 'http://data.gdeltproject.org/gdeltv2/{}.export.csv.zip'
    
    def __init__(self, file_path):
        self.file_path = file_path
        
    def scrape(self, date_beg, date_end):
        month_beg, day_beg, year_beg = date_beg.split('-')
        month_beg, day_beg, year_beg = date_end.split('-')
        print(month_beg)
        

Scrape = Scraper('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Data/zips/GDELT2/')

Scrape.scrape("01-01-2019", "01-02-2019")
