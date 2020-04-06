#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 17:30:35 2019

@author: jamesdawahare
"""

import pandas as pd
from os import path, remove
import os

#filename = 'Data/20191020.export.CSV'
# Get the GDELT field names from a helper file
colnames = pd.read_excel('../Headers/CSV.header.fieldids.xlsx', sheetname='Sheet1',
                        index_col='Column ID', parse_cols=1)['Field Name']
for file in os.listdir('./Data_GDELT_Zips'):

    date_name = file.split('.')[0]
    if path.exists('./Data_GDELT_pickles/{}'.format(date_name)):
        continue
    df = pd.read_csv('./Data_GDELT_Zips/{}'.format(file), sep='\t', header=None, #dtype=str,
                              names=colnames, index_col=['GLOBALEVENTID'])
            
    df.to_pickle('./Data_GDELT_pickles/{}'.format(date_name))
    
    os.remove('./Data_GDELT_Zips/{}'.format(file))
