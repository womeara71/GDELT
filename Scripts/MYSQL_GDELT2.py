# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 08:32:21 2020

@author: 605453
"""

from sqlalchemy import create_engine
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="womeara",
  passwd="ba@154",
  database="practice"
)

cursor = mydb.cursor()

cursor.close()


def create_database(db_name):
    cursor.execute("CREATE DATABASE {}".format(db_name))

#Try and improve this stuff through the chunksize parameter
def df_to_MySQL(df, table_name, db_name):
    engine = create_engine("mysql://womeara:ba@154@localhost/{}".format(db_name), encoding='utf8')
    #engine = create_engine("mysql://womeara:ba@154@localhost/{}?".format(db_name))
    con = engine.connect()
    df.to_sql(name=table_name, con=con, if_exists='append', method='multi', chunksize=5000)
    con.close()