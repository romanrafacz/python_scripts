#!/bin/python
# coding: utf-8
import requests
import json
import psycopg2
import sys

#API variables
api_username=os.environ['API_USERNAME']
api_password=os.environ['API_PASSWORD']
api_url=os.environ['API_URL']
#Database credentials
db_username=os.environ['DB_USERNAME']
db_password=os.environ['DB_PASSWORD']
db_database_api=os.environ['DB_API']

caty = requests.get(api_url, auth=(username, password))
catalog = json.loads(caty.content)

conn = psycopg2.connect(database=db_database_api,host='localhost', port=5432, user='roman', password='snickers')
cur = conn.cursor()

#took out subcategory
SQL = "INSERT INTO location (location_code, location, street, city, zipcode, state, country, timezone) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s);"

counter = 0

for x in catalog:
    try:
        DATA = (x['location']['name'],x['location']['location'],x['location']['street'],x['location']['city'],x['location']['zip'],x['location']['state'],x['location']['country'],x['location']['timezone'])
        cur.execute(SQL, DATA)
        conn.commit()
        print "inserted, " +x['location']['location']
        counter+=1
    except:
        print "not inserted, duplicate, " +x['location']['location']
        conn.rollback()

conn.close()

print 'import success,' + str(counter) + ' records inserted'
