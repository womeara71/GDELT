# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 14:06:07 2020

@author: 605453
"""



class Scraper(object):
    GKG = 'http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip'
    Mentions = 'http://data.gdeltproject.org/gdeltv2/{}.mentions.CSV.zip'
    Events = 'http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip'
    
    def __init__(self, beg_date, end_date):
        beg_month, beg_day, beg_year = beg_date.split("-")
        end_month, end_day, end_year = end_date.split("-")
        
    def scrape:
        
        

prac = Scraper.