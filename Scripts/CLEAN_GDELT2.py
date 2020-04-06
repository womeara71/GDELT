#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 10:24:34 2019

@author: jamesdawahare
"""
import pandas as pd
from os import remove, listdir


def zips2pickles():
    #filename = 'Data/20191020.export.CSV'
    # Get the GDELT field names from a helper file
    #colnames = pd.read_excel('../Headers/header2.0.xlsx', sheetname='Sheet1', parse_cols=1)['tableId']
    colnames_mentions = pd.read_csv('../Headers/schema_csvs/GDELT_2.0_eventMentions_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['0']
    colnames_export = pd.read_csv('../Headers/schema_csvs/GDELT_2.0_Events_Column_Labels_Header_Row_Sep2016.csv')['tableId']
    colnames_gkg = pd.read_csv('../Headers/schema_csvs/GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')['tableId']

    
    for file in listdir('../Data/zips/GDELT2'):
        date_name = file.split('.')[0]+'.'+file.split('.')[1]
        
        if 'mentions' in date_name: 
            df = pd.read_csv('../Data/zips/GDELT2/{}'.format(file), sep='\t', header=None, #dtype=str,
                                          names=colnames_mentions, index_col=['GLOBALEVENTID'])
        if 'events' in date_name:
            df = pd.read_csv('../Data/zips/GDELT2/{}'.format(file), sep='\t', header=None, #dtype=str,
                                          names=colnames_export, index_col=['GLOBALEVENTID'], encoding='utf-8')
        if 'gkg' in date_name:
            df = pd.read_csv('../Data/zips/GDELT2/{}'.format(file), sep='\t', header=None, #dtype=str,
                                          names=colnames_gkg, index_col=['GKGRECORDID'], encoding='cp437')

        df.to_pickle('../Data/pickles/GDELT2/{}'.format(date_name))
        
        
        remove('../Data/zips/GDELT2/{}'.format(file))
    print('Data converted')
        
        
def open_gkg_pickles_into_df():     
    df = pd.concat((pd.read_pickle('../Data/pickles/GDELT2/{}'.format(f)) for f in listdir('../Data/pickles/GDELT2') if 'gkg' in f))
    df.set_index('DocumentIdentifier', inplace=True)
    df.drop_duplicates(inplace=True)
    print('Data read in')
    return df
    
def open_mentions_pickles_into_df():     
    df = pd.concat((pd.read_pickle('../Data/pickles/GDELT2/{}'.format(f)) for f in listdir('../Data/pickles/GDELT2') if 'mention' in f))
    df.drop_duplicates(inplace=True)
    print('Data read in')
    return df

def open_events_pickles_into_df():     
    df = pd.concat((pd.read_pickle('../Data/pickles/GDELT2/{}'.format(f), encoding='utf-8') for f in listdir('../Data/pickles/GDELT2') if 'event' in f))
    df.drop_duplicates(inplace=True)
    print('Data read in')
    return df



