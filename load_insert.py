import time
import psycopg2
import argparse
import re
import csv
import datetime

DBname = "olympus"
DBuser = "zeus"
DBpwd = "Thunder"
BCtable_name = 'breadcrumb'
TPtable_name = 'trip'
tsv_filename = '/home/shared_dir/assignment_1/examples/clients/cloud/python/heatmap_data.tsv'
months = {
    'JAN': 1,
    'FEB': 2,
    'MAR': 3,
    'APR': 4,
    'MAY': 5,
    'JUN': 6,
    'JUL': 7,
    'AUG': 8,
    'SEP': 9,
    'OCT': 10,
    'NOV': 11,
    'DEC': 12,
}

# connect to the database
def dbconnect ():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = True
	return connection

def createTables (conn):

    with conn.cursor() as cursor:
        # Create BreadCrumb Table
        cursor.execute(f"""
        DROP TABLE IF EXISTS {BCtable_name};
        	CREATE TABLE {BCtable_name} (
                tstamp  TIMESTAMP,
                latitude FLOAT,
                longitude FLOAT,
                direction INTEGER,
                speed FLOAT,
                trip_id INTEGER
                );
                CREATE INDEX idx_{BCtable_name}_State ON {BCtable_name}(tstamp);
        """)
        print(f"Created {BCtable_name}")
        # Create Trip Table 
        cursor.execute(f"""
        DROP TABLE IF EXISTS {TPtable_name};
        	CREATE TABLE {TPtable_name} (
                trip_id INTEGER PRIMARY KEY,
                route_id INTEGER,
                vehicle_id INTEGER,
                service_key TEXT,
                direction INTEGER,
                );
        """)
        print(f"Created {TPtable_name}")
       
def insert_breadcrumb (conn, filename):
    cur = conn.cursor()
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip the header row
        for row in reader:
            cur.execute("INSERT INTO breadcrumb VALUES (%s,%s,%s,%s,%s,%s)", row)
    conn.commit()
       
def insert_trip (conn, filename):
    cur = conn.cursor()
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip the header row
        for row in reader:
            trip_id = int(row[0])
            # Check if pk exists already
#            cur.execute("select exists(select * from trip where trip_id = %s)", (trip_id,))
#            pk_exists = cur.fetchone()
#            if pk_exists[0] is False:
            route_id = int(row[1])
            vehicle_id = int(row[2])
            service_key = row[3]
            s_date = service_key.split('-')
            year = '20{}'.format(s_date[2])
            month = months[s_date[1]]
            day = s_date[0].lstrip('0')
            dow = datetime.date(int(year), int(month), int(day)).weekday()
            if dow < 5:
                service_key = "Weekday"
            elif dow == 5:
                service_key = "Saturday"
            elif dow == 6:
                service_key = "Sunday"
            direction = row[4]
            #cur.execute("INSERT INTO trip VALUES (%s,%s,%s,%s,%s)", (trip_id, route_id, vehicle_id, service_key, direction))
            cur.execute("INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (trip_id) DO UPDATE SET trip_id = %s, route_id = %s, vehicle_id = %s, service_key = %s, direction = %s", (trip_id, route_id, vehicle_id, service_key, direction, trip_id, route_id, vehicle_id, service_key, direction))
    conn.commit()

def insert_event (conn, filename):
    cur = conn.cursor()
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip the header row
        for row in reader:
            trip_id = int(row[0])
            vehicle_id = int(row[1])
            route_id = int(row[2])
            direction = int(row[3])
            if direction == 0:
                direction = "Out"
            elif direction == 1:
                direction = "Back"
            service_key = row[4]
            if service_key == "U":
                service_key = "Sunday"
            elif service_key == "S":
                service_key = "Saturday"
            elif service_key == "W":
                service_key = "Weekday"
            #print("Values:", trip_id, vehicle_id, route_id, direction, service_key)
            cur.execute("INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (trip_id) DO UPDATE SET trip_id = %s, route_id = %s, vehicle_id = %s, service_key = %s, direction = %s", (trip_id, route_id, vehicle_id, service_key, direction, trip_id, route_id, vehicle_id, service_key, direction))
    conn.commit()

def load_data (BC_file, TP_file, create_table_flag):
    conn = dbconnect() 
#    if create_table_flag:
#        createTables(conn)
    print("Loading breadcrumbs into database...")
    insert_trip(conn, TP_file)
    insert_breadcrumb(conn, BC_file)

def load_event_data (EV_file):
    conn = dbconnect() 
    print("Loading stop event data into database...")
    insert_event(conn, EV_file)

