# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 14:06:07 2020

@author: 605453
"""

import datetime
import urllib.request
import pandas as pd
import numpy as np
import socket
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import MEDIUMTEXT

socket.setdefaulttimeout(15)


class Scraper(object):
    GKG_url = 'http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip'
    Mentions_url = 'http://data.gdeltproject.org/gdeltv2/{}.mentions.CSV.zip'
    Events_url = 'http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip'
    
    colnames_gkg = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['tableId']
    colnames_mentions = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_eventMentions_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['0']
    colnames_events = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv')['tableId']
    
    dict_gkg = {colnames_gkg[0]: MEDIUMTEXT}
    dict_gkg.update({x: MEDIUMTEXT for x in colnames_gkg[5:]})
    dict_mentions = {x: MEDIUMTEXT for x in colnames_mentions[3:]}
    dict_events = {x: MEDIUMTEXT for x in colnames_events[5:]}    
    
    def __init__(self, beg_date, end_date, folder):
        self.beg_month, self.beg_day, self.beg_year = [int(x) for x in beg_date.split("-")]
        self.end_month, self.end_day, self.end_year = [int(x) for x in end_date.split("-")]
        self.folder =  folder
        
    def pull(self, url_type, date, dtype):
        print("Start the Pull")
        url = url_type.format(date)
        file_location = '{}{}.{}.CSV.zip'.format(self.folder, date, dtype)
        try:
            print("Really Starting the pull")
            urllib.request.urlretrieve(url,file_location)
            print("Pull 1")
            return file_location
        except:
            print("Pull 2")
            return np.nan
        
    def pandafy(self, file_location, colnames, GKG_drop=False):
        try:
            df = pd.read_csv(file_location, sep='\t', header=None, names=colnames, encoding='utf-8')
        except:
            df = pd.read_csv(file_location, sep='\t', header=None, names=colnames, encoding='latin-1')
        print("Converted to pandas")
        return df
                
    def insert(self, df, table_name, db_name):
        engine = create_engine("mysql://womeara:ba@154@localhost/{}?charset=utf8mb4".format(db_name), encoding = 'utf-8', echo=False)
        con = engine.connect()
        if table_name == "mentions":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema="practice", index=False, dtype=self.dict_mentions)
        elif table_name == "events":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema="practice", index=False, dtype=self.dict_events)
        elif table_name == "gkg":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema="practice", index=False, dtype=self.dict_gkg)
        print("Inserted")
        con.close()
        
    def execute(self, url, date, table, colnames, GKG_drop = False):
        print(table)
        result = self.pull(self.GKG_url, date, table)
        df = self.pandafy(result, colnames, GKG_drop = False)
        self.insert(df, table, "practice")
            
    def scrape(self, GKG=False, Mentions=False, Events=False):
        start = datetime.datetime(year = self.beg_year, month = self.beg_month, day = self.beg_day, hour=00, minute=00)
        end = datetime.datetime(year = self.end_year, month = self.end_month, day = self.end_day, hour=00, minute=00)
        days_to_collect = end - start
        date_list = [end - datetime.timedelta(minutes=15*x) for x in range(1, days_to_collect.days*96)]
        date_list = list(map(lambda x: x.strftime("%Y%m%d%H%M") + '00', date_list))
        
        
        for date in date_list:
            print(date)
            if GKG:
                print("GKG")
                result = self.pull(self.GKG_url, date, "gkg")
                df = self.pandafy(result, self.colnames_gkg, GKG_drop=True)
                self.insert(df, "gkg", "practice")
                
            if Mentions:
                self.execute(self.Mentions_url, date, "mentions", self.colnames_mentions)
                #print("Mentions")
                #result = self.pull(self.Mentions_url, date, "mentions")
                #df = self.pandafy(result, self.colnames_mentions, 'GLOBALEVENTID')
                #self.insert(df, "mentions", "practice")   
                
            if Events:
                print("Events")
                result = self.pull(self.Events_url, date, "events")
                df = self.pandafy(result, self.colnames_events)
                self.insert(df, "events", "practice")  


        
prac = Scraper("05-20-2019", "05-24-2019",
               "C:/Users/605453/Downloads/")

prac.scrape(GKG=True, Mentions=True, Events=True)


