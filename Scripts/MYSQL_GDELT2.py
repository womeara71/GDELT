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
  database="gdelt"
)

cursor = mydb.cursor()



def create_database(db_name):
    cursor.execute("CREATE DATABASE {}".format(db_name))


def df_to_MySQL(df, table_name, db_name):
    engine = create_engine("mysql://womeara:ba@154@localhost/{}?charset=utf8".format(db_name))
    con = engine.connect()
    df.to_sql(name=table_name, con=con, if_exists='append')
    con.close()
    

