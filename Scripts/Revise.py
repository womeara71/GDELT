# -*- coding: utf-8 -*-
"""
Created on Sat Jul  4 14:06:07 2020

@author: 605453
"""

import datetime
import urllib.request
import pandas as pd
import socket
import sys
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import MEDIUMTEXT, VARCHAR
from os import remove

socket.setdefaulttimeout(15)


class Scraper(object):
    GKG_url = 'http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip'
    Mentions_url = 'http://data.gdeltproject.org/gdeltv2/{}.mentions.CSV.zip'
    Events_url = 'http://data.gdeltproject.org/gdeltv2/{}.export.CSV.zip'
    
    colnames_gkg = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['tableId']
    colnames_mentions = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_eventMentions_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['0']
    colnames_events = pd.read_csv('C:/Users/605453/Documents/Projects/Firesail/Save Our Jobs/Part 2/Archive/Headers/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv')['tableId']
    
    dict_gkg = {colnames_gkg[0]: VARCHAR(255)}
    dict_gkg.update({x: MEDIUMTEXT(collation = 'utf8mb4_bin') for x in colnames_gkg[5:]})
    dict_mentions = {x: MEDIUMTEXT(collation = 'utf8mb4_bin') for x in colnames_mentions[3:]}
    dict_events = {x: MEDIUMTEXT(collation = 'utf8mb4_bin') for x in colnames_events[5:]}    
    
    error_log = "C:\\Users\\605453\\Documents\\GDELT\\Errors.txt"
    
    def __init__(self, beg_date, end_date, folder, db):
        self.beg_month, self.beg_day, self.beg_year = [int(x) for x in beg_date.split("-")]
        self.end_month, self.end_day, self.end_year = [int(x) for x in end_date.split("-")]
        self.folder = folder
        self.db = db
        
    def pull(self, url_type, date, dtype):
        url = url_type.format(date)
        file_location = '{}{}.{}.CSV.zip'.format(self.folder, date, dtype)
        urllib.request.urlretrieve(url,file_location)
        return file_location

    def pandafy(self, file_location, colnames, index, GKG_drop=False):
        try:
            df = pd.read_csv(file_location, sep='\t', header=None, names=colnames, encoding='utf-8')
        except:
            df = pd.read_csv(file_location, sep='\t', header=None, names=colnames, encoding='latin-1')
        remove(file_location)
        df = df[df.iloc[:,0].astype(str).apply(lambda x: len(x) < 255)]
        df = df.set_index(index)
        return df
                
    def insert(self, df, table_name, db_name, index):
        engine = create_engine("mysql://womeara:ba@154@localhost/{}?charset=utf8mb4".format(db_name), echo=False)
        con = engine.connect()
        if table_name == "mentions":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema=db_name, index_label = index, dtype=self.dict_mentions)
        elif table_name == "events":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema=db_name, index_label = index, dtype=self.dict_events)
        elif table_name == "gkg":
            df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=50, method="multi", schema=db_name, index_label = index, dtype=self.dict_gkg)
        con.close()
        
    def execute(self, url, date, table, colnames, index, GKG_drop = False):
        try:
            result = self.pull(url, date, table)
            print(result)
            df = self.pandafy(result, colnames, index, GKG_drop = False)
            self.insert(df, table, self.db, index)
        except:
            error  = "Fail:{}-{}".format(date, table)
            print(error)
            print(sys.exc_info()[0])
            Errors.append(error)
            
    def scrape(self, GKG=False, Mentions=False, Events=False):
        start = datetime.datetime(year = self.beg_year, month = self.beg_month, day = self.beg_day, hour=00, minute=00)
        end = datetime.datetime(year = self.end_year, month = self.end_month, day = self.end_day, hour=00, minute=00)    
        days_to_collect = end - start
        date_list = [end - datetime.timedelta(minutes=15*x) for x in range(1, days_to_collect.days*96+1)]
        date_list = list(map(lambda x: x.strftime("%Y%m%d%H%M") + '00', date_list))
        
        global Errors
        Errors = []
        
        for date in date_list:
            print(date)
            if GKG:
                self.execute(self.GKG_url, date, "gkg", self.colnames_gkg, 'GKGRECORDID')

            if Mentions:
                self.execute(self.Mentions_url, date, "mentions", self.colnames_mentions, 'GLOBALEVENTID')
                
            if Events:
                self.execute(self.Events_url, date, "events", self.colnames_events, 'GLOBALEVENTID')
                
            with open(self.error_log, "w") as outfile:
                outfile.write("\n".join(Errors))
        
        now = datetime.datetime.now()
        error_log_final = "C:\\Users\\605453\\Documents\\GDELT\\Error Logs\\Errors_" + now.strftime("%d%m%Y_%H%M%S") + ".txt"
        with open(error_log_final, "w") as outfile:
                outfile.write("\n".join(Errors))
    



'''
To Do's:
1. Function to Reinsert on Strange Errors
2. Set Date Column as index???
4. Read up on character sets and mysql architecture
5. Change to spit out error link
    
1. More Agnostic to RDBMS
2. Refactor passing of variables
4. Too many if's and too many try/ excepts
6. Make portable
8. Flip beginning and end date as it's confusing
8. Relative references for file paths
'''