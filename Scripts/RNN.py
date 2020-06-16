# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 11:25:14 2020

@author: 605453
"""
import mysql.connector
import pandas as pd
from keras.models import Sequential, load_model
from keras.layers import LSTM, Dropout, Dense
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import confusion_matrix
from imblearn.over_sampling import SMOTE
import numpy as np


####### Functions
def data_pull(variables, conditions):
    mydb = mysql.connector.connect(host="localhost", user="womeara", passwd="ba@154", database="gdelt")
    df = pd.read_sql("SELECT {} FROM events e INNER JOIN mentions m ON {}".format(variables, conditions),con=mydb)
    mydb.close()
    print("Data Returned")
    return df

def clean_df(df):
    dummydf = pd.get_dummies(df, columns=['EventCode'])
    dummydf = dummydf.groupby('MentionTimeDate').sum()
    dummydf = (dummydf - dummydf.mean())/dummydf.std()
    dummydf['Protest Occuring'] = 0
    dummydf.index = pd.to_datetime(dummydf.index, format = "%Y%m%d%H%M%S")
    print("Data Cleaned")
    return dummydf

def assign_dates(df, event_dates):
    for x in event_dates:
        mask = (df.index > x[0]) & (df.index <= x[1])
        df.loc[mask, 'Protest Occuring'] = 1
    df.loc[~df["Protest Occuring"]==1, "Protest Occuring"] = 0
    print("Dates Assigned")
    return df
    
def return_split(df):
    X = df.iloc[:,:-1]
    y = df.iloc[:,-1]
    oversample = SMOTE()
    X, y = oversample.fit_resample(X, y)
    step = 5
    X_new = np.array([X.iloc[i:i+step].values for i in range(0,len(X) - step, step)])
    y_new = np.array([y[i+step-1] for i in range(0,len(X) - step, step)])
    y_new  = y_new.reshape(len(y_new), 1)
    number_of_rows = X_new.shape[0]
    random_rows = np.random.choice(number_of_rows, size=round(.66*number_of_rows), replace=False)
    opp_random_rows = np.array([x for x in range(len(X_new)) if x not in random_rows])
    X_train = X_new[random_rows, :]
    X_test = X_new[opp_random_rows, :]
    y_train = y_new[random_rows]
    y_test = y_new[opp_random_rows]
    return X_train, X_test, y_train, y_test 

def create_model(df_assigned):
    model = Sequential()
    model.add(LSTM(32, return_sequences=False, 
                   dropout=0.3,
                   input_shape=(X_train.shape[1], X_train.shape[2])))
    #model.add(Dense(32))
    model.add(Dense(1, activation='sigmoid'))
    model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])
    print("Model Compiled")
    return model
    

####### Data Manipulation
variables = 'm.MentionTimeDate, e.EventCode'
conditions = "m.GlobalEventID=e.GlobalEventID AND e.ActionGeo_CountryCode = 'US'"

event_dates = [["2019-05-25 15:00:00","2019-05-25 17:00:00"],
               ["2019-05-26 15:00:00","2019-05-26 17:00:00"],
               ["2019-06-01 15:00:00","2019-06-01 17:00:00"],
               ["2019-06-08 15:00:00","2019-06-08 17:00:00"],
               ["2019-06-09 18:00:00","2019-06-09 19:00:00"],
               ["2019-06-09 14:00:00","2019-06-09 16:00:00"],
               ["2019-06-12 16:00:00","2019-06-12 17:00:00"],
               ["2019-06-13 16:00:00","2019-06-13 18:00:00"],
               ["2019-06-14 16:00:00","2019-06-14 18:00:00"],
               ["2019-06-15 15:00:00","2019-06-15 17:00:00"],
               ["2019-06-15 14:30:00","2019-06-15 16:30:00"],
               ["2019-06-18 15:00:00","2019-06-18 18:00:00"],
               ["2019-06-20 22:00:00","2019-06-20 23:00:00"],
               ["2019-06-21 15:00:00","2019-06-21 18:00:00"]]

df_orig = data_pull(variables, conditions)
df_clean = clean_df(df_orig)
df_assigned = assign_dates(df_clean, event_dates)
X_train, X_test, y_train, y_test = return_split(df_assigned)


#### Keras Model
model = create_model(X_train)

history = model.fit(X_train,  y_train, 
                    batch_size=32, epochs=35,
                    class_weight = {0:0.2, 1:0.8})

#model.save("C:\\Users\\605453\\Documents\\GDELT\\Models\\model")
#jenk = load_model("C:\\Users\\605453\\Documents\\GDELT\\Models\\model")

results = model.evaluate(X_test, y_test, batch_size=8)

guess = model.predict(X_test)
guess = [x>0.5 for x in guess]

cm = confusion_matrix(y_test, guess)