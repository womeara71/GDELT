# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:07:21 2020

@author: 605453
"""

import os
os.chdir('C:\\Users\\605453\\GDELT\\Scripts')

import pandas as pd
import numpy as np
import mysql.connector
from MYSQL_GDELT2 import create_database, df_to_MySQL
from CLEAN_GDELT2 import open_gkg_pickles_into_df, open_mentions_pickles_into_df, open_events_pickles_into_df, zips2pickles 
from sklearn.model_selection import train_test_split 
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor  
from sklearn import metrics
from sklearn.ensemble import GradientBoostingRegressor


# Convert Zipfiles pickles. Concatenate all files into representative dataframes
zips2pickles()
df_mentions = open_mentions_pickles_into_df()
df_to_MySQL(df_mentions, 'mentions', 'gdelt')

df_gkg = open_gkg_pickles_into_df()
df_to_MySQL(df_gkg, 'gkg', 'gdelt')

df_events = open_events_pickles_into_df()
df_events = df_events[int(len(df_events)/2):]
#df_events[df_events.EventCode.astype(str).apply(lambda x: '--' in x)]
df_to_MySQL(df_events, 'events', 'gdelt')


df_gkg.set_index('DocumentIdentifier', inplace=True)

df_prac = df_gkg[:100]
# Dataframes to MySQL
#create_database('GDELT')
df_to_MySQL(df_prac, 'gkg', 'gdelt')


############################Data Exploration: Through MySQL#######################################
#FIPS10-4 is the code for countries: https://en.wikipedia.org/wiki/List_of_FIPS_country_codes
mydb = mysql.connector.connect(
  host="localhost",
  user="womeara",
  passwd="ba@154",
  database="gdelt"
)

cursor = mydb.cursor()




###############################AutoCorrelation UK Protests########################################

###Pre-Processing for autocorrelation with a one day lag
# =============================================================================
# cursor.execute('SHOW columns FROM events')
# headers = cursor.fetchall()
# headers = [header[0] for header in headers]
# cursor.execute("SELECT * FROM events WHERE ActionGeo_CountryCode = 'UK'")
# df = pd.DataFrame(cursor.fetchall(), columns=headers)
# df = df[df.EventCode.astype(str).str.contains('^14[0-5]', regex=True)]
# IDs = df.GLOBALEVENTID
# 
# List_mentions = []
# for ID in IDs:
#     cursor.execute("SELECT GLOBALEVENTID, EventTimeDate, GoldsteinScale FROM mentions WHERE GLOBALEVENTID = {}".format(ID)) 
#     df_temp = pd.DataFrame(cursor.fetchall())
#     List_mentions.append(df_temp)
#     print('Done')
#     
# df_mentions = pd.concat(List_mentions, columns=['GLOBALEVENTID', 'EventTimeDate', 'GoldsteinScale'])    
# df_mentions = df.rename(columns = {0: 'GLOBALEVENTID', 1: 'EventTimeDate'})
# 
# df_mentions['EventTimeDate'] = df_mentions['EventTimeDate'].astype(str).str[:8]
# df_m_group = df_mentions.groupby('EventTimeDate').size()
# df_m_group_lag = df_m_group[1:]
# df_m_group_present = df_m_group[:-1]
# y = df_m_group_present.values.reshape(-1,1)
# X = df_m_group_lag.values.reshape(-1,1)
# =============================================================================

#Pre-processing for Data with Protest Types
variables = 'm.GlobalEventID, m.MentionTimeDate, m.EventTimeDate, e.GoldsteinScale, e.EventCode'
conditions = "m.GlobalEventID=e.GlobalEventID AND (e.EventCode REGEXP '^14[0-5]' AND e.ActionGeo_CountryCode = 'UK')"


cursor.execute("SELECT {} FROM events e INNER JOIN mentions m ON {}".format(variables, conditions))
df = pd.DataFrame(cursor.fetchall(), columns=['Global Event ID', 'MentionTimeDate', 'EventTimeDate', 'Goldstein Scale', 'Event Code'])
df['MentionTimeDate'] = df['MentionTimeDate'].astype(str).str[:8]
df['Event Code'] = df['Event Code'].astype(str).str[:3]
df.drop(['Global Event ID','EventTimeDate', 'Goldstein Scale'], axis=1, inplace=True)
df['Count'] = 1
df_piv = pd.pivot_table(df, values='Count', index=['MentionTimeDate'], columns=['Event Code'], aggfunc=np.sum)
df_piv.fillna(0, inplace=True)
y = df_piv['145'].values.reshape(-1,1)[:-1]
X = df_piv.drop('145', axis=1)[1:]


#Regression 
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.3)
#regression = LinearRegression()
#regression = DecisionTreeRegressor(random_state = 0) 
#regression = RandomForestRegressor(n_estimators = 100, random_state = 0)
regression = GradientBoostingRegressor(learning_rate=1.0)
regression.fit(X_train, y_train)
y_pred = regression.predict(X_test)
df = pd.DataFrame({'Actual': y_test.flatten(), 'Predicted': y_pred.flatten()})

print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, y_pred))  
print('Mean Squared Error:', metrics.mean_squared_error(y_test, y_pred))  
print('Root Mean Squared Error:', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))


#Decision Tree



#Decision Tree with XG Boost

###########################Data Exploration: Will be done through MySQL###########################
# Create Final DataFrame
df_final = pd.DataFrame(columns=['Event Time', 'Event Location', 'Number of Mentions', 'Best Url'])
    
# Only take protest mentions, group them by day.
df_protest = df_events[df_events.EventCode.astype(str).str.contains('^14[0-5]', regex=True)]
df_protest_us = df_protest[df_protest.ActionGeo_CountryCode == 'US']
df_protest_us = df_mentions.loc[df_protest_us.index]
df_protest_us['Day'] = list(map(lambda x: x[:8], df_protest_us.MentionTimeDate.astype(str)))
df_mentions_days = df_protest_us[['Day']].groupby('Day').size()
top_days = [index for index in df_mentions_days.nlargest(3).index]
top_events = [df_protest_us[df_protest_us.MentionTimeDate.astype(str).str.contains(x)].reset_index()[['GLOBALEVENTID']].groupby('GLOBALEVENTID').size().idxmax() for x in top_days]
top_urls = [df_protest_us.loc[x].MentionIdentifier.iloc[0] for x in top_events]

# Explore info on the protest with the gkg
df_gkg_protest = df_gkg[df_gkg.DocumentIdentifier == 'https://www.basildonstandard.co.uk/news/national/18069424.tens-thousands-children-skip-school-climate-strike-protest/']
df_gkg_protest = df_gkg[df_gkg.DocumentIdentifier == any(top_urls)]

#df_mentions[df_mentions.SOURCEURL.str.contains('https://www.times-series.co.uk/news/national/18070483.knife-attacker-fake-suicide-vest-killed-police-london-bridge/')]

# GKG: Create Dataframe for all articles which have count information on protests. Count is a placeholder for Theme
# =============================================================================
# word_list = ['PROTEST']
# df_protest = df_gkg[df_gkg.V2Counts.apply(lambda observation: any(word in str(observation) for word in word_list))]
# =============================================================================

# Picking out Spikes
# =============================================================================
# df_protest['COUNT'] = 1
# df_protest_count = df_protest[['DATE', 'COUNT']]
# df_protest_count = df_protest_count.groupby('DATE', as_index=False).sum()
# #df_protest_count.drop(df_protest_count.tail(1).index,inplace=True)
# 
# sdev = df_protest_count['COUNT'].std()
# mean = df_protest_count['COUNT'].mean()
# df_protest_count[df_protest_count['COUNT'] > mean + sdev*2]
# 
# max_period = df_protest_count.loc[df_protest_count.COUNT.idxmax()].DATE
# df_protest_max = df_protest[df_protest.DATE == max_period]
# =============================================================================


# Create Dataframe that counts the amount of time a protest is mentioned in the news
# =============================================================================
# df_protest = df_events[df_events.EventCode.astype(str).str.contains('^14[0-5]', regex=True)]
# df_protest_uk = df_protest[df_protest.ActionGeo_CountryCode == 'US']
# protest_mentions = df_mentions.loc[df_protest_uk.index]
# protest_mentions['Count'] = 1
# protest_mentions = protest_mentions.reset_index()
# protest_mentions_group = protest_mentions[['GLOBALEVENTID','Count']].groupby('GLOBALEVENTID').sum()
# protest_mentions_group.sort_values(by='Count', inplace=True)
# =============================================================================

# Explore Top Mentions
# =============================================================================
# for url in protest_mentions[protest_mentions['GLOBALEVENTID']==889620138]['MentionIdentifier']:
#     print(url)
# =============================================================================

#df_protest_us[df_protest_us.MentionTimeDate.astype(str).str.contains(x)].reset_index()[['GLOBALEVENTID']].groupby('GLOBALEVENTID').size().idxmax()



### Jenk code to find error columns
# =============================================================================
# y = 0
# errors=0
# errors_list=[]
# for x in list(range(1000, len(df_gkg), 1000)):
#     try:
#         df_to_MySQL(df_gkg[y:x], 'gkg', 'gdelt')
#         print('DONE')
#     except Exception as e:
#         print(e)
#         errors_list.append(e)
#         errors+=1
#     y = x
# =============================================================================