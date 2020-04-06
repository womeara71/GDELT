#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 10:44:18 2019

@author: jamesdawahare
"""

import glob
import pandas as pd

filename = 'Data/20191020.export.CSV'

# Get the GDELT field names from a helper file
colnames = pd.read_excel('Data/CSV.header.fieldids.xlsx', sheetname='Sheet1', 
                         index_col='Column ID', parse_cols=1)['Field Name']

# Build DataFrames from each of the intermediary files
file = glob.glob(filename)

DFlist = pd.read_csv(filename, sep='\t', header=None, dtype=str,
                              names=colnames, index_col=['GLOBALEVENTID'])

# Merge the file-based dataframes and save a pickle
#DF = pd.concat(DFlist)
#DF.to_pickle(local_path+'backup'+fips_country_code+'.pickle')    
    
# once everythin is safely stored away, remove the temporary files
#for active_file in files:
#    os.remove(active_file)