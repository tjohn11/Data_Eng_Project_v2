import time
import psycopg2
import argparse
import re
import csv
import datetime

DBname = "olympus"
DBuser = "zeus"
DBpwd = "Thunder"
# filename = '/home/shared_dir/assignment_1/examples/clients/cloud/python/heatmap_data.tsv'

def generate_tsv(filename):
    # connect to the database
    connection = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd,
    )
    connection.autocommit = True
    cur = connection.cursor()

    print("Storing heatmap data")

    # All data
    # cur.execute("COPY (SELECT latitude, longitude, speed FROM breadcrumb) TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 1
    # cur.execute("COPY (SELECT latitude, longitude, speed FROM breadcrumb WHERE trip_id = 169664890) TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Vizualization 2
    # cur.execute("COPY (SELECT bc.latitude, bc.longitude, bc.speed FROM breadcrumb as bc, trip as t WHERE t.route_id = 65 AND bc.tstamp::text between '2020-10-21 18:00:00' AND '2020-10-21 20:00:00' AND bc.trip_id = t.trip_id AND t.direction = 'Out') TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 3
    # cur.execute("COPY (SELECT bc.latitude, bc.longitude, bc.speed FROM breadcrumb as bc, trip as t WHERE bc.tstamp::text between '2020-10-21 11:00:00' AND '2020-10-21 13:00:00' AND bc.trip_id = t.trip_id AND t.direction = 'Out' AND t.route_id = 71) TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 4
    # cur.execute("COPY (SELECT latitude, longitude, speed FROM breadcrumb WHERE trip_id = 170871950) TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 5a
    cur.execute("COPY (SELECT latitude, longitude, speed FROM breadcrumb as b, trip as t WHERE t.trip_id = b.trip_id AND t.vehicle_id = 2244 AND b.tstamp::date::text = '2020-10-21') TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 5b
    # cur.execute("COPY (SELECT bc.latitude, bc.longitude, bc.speed FROM breadcrumb as bc, (SELECT trip_id FROM breadcrumb WHERE speed >= 35) as q WHERE bc.trip_id = q.trip_id) TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

    # Visualization 5c
    # cur.execute("COPY (SELECT latitude, longitude, speed FROM breadcrumb WHERE tstamp::text > '2020-10-21 18:30') TO '%s' WITH DELIMITER E'\t' null as ';'" % filename)

