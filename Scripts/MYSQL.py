# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 08:06:31 2020

@author: 605453
"""

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="womeara",
  passwd="ba@154",
  database="gdelt"
)

cursor = mydb.cursor()

#  Create a database
# =============================================================================
# cursor.execute("CREATE DATABASE datacamp")
# cursor.execute("SHOW DATABASES")
# databases = cursor.fetchall()
# print(databases)
# =============================================================================

# Create tables
# =============================================================================
# cursor.execute("CREATE TABLE users (name VARCHAR(255), user_name VARCHAR(255))")
# cursor.execute("SHOW TABLES")
# 
# tables = cursor.fetchall()
# for table in tables:
#     print(table)
# =============================================================================
    
query= "INSERT INTO users (name, user_name) VALUES (%s, %s)"
values = [
        ('Peter', 'peter'),
        ('Amy', 'amy'),
        ('Michael', 'michael'),
        ('Hannah', 'hannah')
        ]
cursor.executemany(query, values)
mydb.commit()

print(cursor.rowcount, "record insterted")


#######################Helpful SQL Commands####################################

### Alter Column DATA TYPE
cursor.execute('ALTER TABLE gkg MODIFY V2LOCATIONS LONGTEXT')


### Show Column Names
cursor.execute('DESCRIBE gkg')

    
### Kill Connections


### Drop a Table


### Alter Database Charactersets and Collations (Encoding)
mydb.set_charset_collation('utf8')
cursor.execute("ALTER TABLE events CONVERT TO CHARACTER SET utf8")
cursor.execute("ALTER TABLE events COLLATE utf8_general_ci")


### Join
cursor.execute("SELECT * FROM users INNER JOIN ids USING (name)")

### Deleting Rows
cursor.execute('DELETE FROM users ORDER BY user_name LIMIT 3')

### Advanced Regex
cursor.execute("SELECT EventCode FROM events WHERE EventCode REGEXP '14[0-5]'")

### Change Timeout 
cursor.execute("SET GLOBAL net_write_timeout = 120")
cursor.execute("SHOW VARIABLES LIKE 'wait_timeout'")

### Check all Database Encodings on Server
cursor.execute("SELECT * FROM information_schema.SCHEMATA")


### Elaborate-ish Join
variables = 'm.GlobalEventID, m.MentionTimeDate, m.EventTimeDate, e.EventCode, e.IsRootEvent, e.AvgTone'
conditions = "m.GlobalEventID=e.GlobalEventID AND (e.EventCode REGEXP '^14[0-5]' AND e.ActionGeo_CountryCode = 'US')"



