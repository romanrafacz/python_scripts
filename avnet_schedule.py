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
SQL = "INSERT INTO courses2 (name, start_date, end_date, duration, duration_unit, type_id, type_url, title, vendor, category, price) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

#for x in catalog:
#    DATA=(x[‘name’], x[‘startDate’], x[‘endDate’], x[‘duration’], x[‘durationUnit’], x[‘location’][‘name’], x[‘typeId’], x[‘typeURL’], x[‘title’], x[‘vendor’], x[‘category’], x[‘price’])
#    DATA=(x['name'])
#    cur.execute(SQL2, DATA)
SQL3 = "INSERT INTO schedule (schedule_code, start_date, end_date, course_code, title, course_url, category, subcategory, status, location_code) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

for x in catalog:
    try:
        DATA = (x['name'], x['startDate'], x['endDate'], x['typeId'], x['title'], x['typeURL'], x['category'], x['subcategory'], x['status'], x['location']['name'])
        cur.execute(SQL3, DATA)
        conn.commit()
    except:
        print 'import error'
        print x['location']
#    e = sys.exc_info()[0]
#    print ( "<p>Error: %s</p>" % e )

conn.close()

print 'import success'
