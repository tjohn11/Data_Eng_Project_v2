#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import csv
import time
from load_insert import load_data

event_csv_file = 'event_data.csv'

# def insertVID(data):
#     if data['VEHICLE_ID'] in working_data:
#         pass
#         # Do something
#     else:
#         working_data.update({data['VEHICLE_ID']: 
#             {   
#                 'ACT_TIME': data['ACT_TIME'],
#                 'VELOCITY': data['VELOCITY'],
#                 'DIRECTION': data['DIRECTION'],
#             }
#         })

def convert_time(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(int(seconds)))

# def validateData(data, prev_time):
    # Validations

    # DB key DNE assertions
#     try:
        assert(data['EVENT_NO_TRIP'] is not '')
    except AssertionError:
        # print('***VEHICLE ID DNE EXCEPTION AT VEHICLE ID: %s ***' % data['VEHICLE_ID'])
        return 1
    try:
        assert(data['EVENT_NO_STOP'] is not '')
    except AssertionError:
        # print('***VEHICLE ID DNE EXCEPTION AT VEHICLE ID: %s ***' % data['VEHICLE_ID'])
        return 1
    try:
        assert(data['VEHICLE_ID'] is not '')
    except AssertionError:
        # print('***VEHICLE ID DNE EXCEPTION AT VEHICLE ID: %s ***' % data['VEHICLE_ID'])
        return 1
    try:
        assert(data['OPD_DATE'] is not '')
    except AssertionError:
        # print('***OPD_DATE DNE EXCEPTION AT OPD DATE: %s ***' % data['OPD DATE'])
        return 1

    # Data corruption assertions
    try:
        assert(data['ACT_TIME'] is not '')
    except AssertionError:
        # print('***ACT TIME DNE EXCEPTION AT ACT TIME: %s ***' % data['ACT_TIME'])
        return 1
    try:
        assert(int(data['ACT_TIME']) >= prev_time)
        prev_time = int(data['ACT_TIME'])
    except AssertionError:
        # print('***ACT TIME NON-LINEAR EXCEPTION AT ACT TIME: %s ***' % data['ACT_TIME'])
        return 1

    try:
        assert(data['DIRECTION'] is not '')
    except AssertionError:
        # print('***DIRECTION DNE EXCEPTION AT DIRECTION: %s ***' % data['DIRECTION'])
        return 1
    try:
        assert(int(data['DIRECTION']) >= 0 and int(data['DIRECTION']) <= 359)
    except AssertionError:
        # print('***DIRECTION RANGE EXCEPTION AT DIRECTION: %s ***' % data['DIRECTION'])
        return 1

    try:
        assert(data['VELOCITY'] is not '')
    except AssertionError:
        # print('***VELOCITY DNE EXCEPTION AT VELOCITY: %s ***' % data['VELOCITY'])
        return 1
    try:
        assert(int(data['VELOCITY']) >= 0 and int(data['VELOCITY']) <= 100)
    except AssertionError:
        # print('***VELOCITY RANGE EXCEPTION AT VELOCITY: %s ***' % data['VELOCITY'])
        return 1
    return 0 # passed all assertions


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    working_data = {}

    # Process messages
    # total_count = 0
    #f = open('consumer_output.json', 'w')
    breadcrumb_csv = open(BC_file, 'w')
    trip_csv = open(TP_file, 'w')
    failed_csv = open('failed_data.csv', 'w')
    breadcrumb_headers = [
            'tstamp',
            'latitude',
            'longitude',
            'direction',
            'speed',
            'trip_id'
    ]
    trip_headers = [
            'trip_id',
            'route_id',
            'vehicle_id',
            'service_key',
            'direction'
    ]
    failed_headers = [
            'EVENT_NO_TRIP',
            'EVENT_NO_STOP',
            'OPD_DATE',
            'VEHICLE_ID',
            'METERS',
            'ACT_TIME',
            'VELOCITY',
            'DIRECTION',
            'RADIO_QUALITY',
            'GPS_LONGITUDE',
            'GPS_LATITUDE',
            'GPS_SATELLITES',
            'GPS_HDOP',
            'SCHEDULE_DEVIATION'
    ]

    try:
        breadcrumb_writer = csv.DictWriter(breadcrumb_csv, fieldnames=breadcrumb_headers)
        breadcrumb_writer.writeheader()
        trip_writer = csv.DictWriter(trip_csv, fieldnames=trip_headers)
        trip_writer.writeheader()
        failed_writer = csv.DictWriter(failed_csv, fieldnames=failed_headers)
        failed_writer.writeheader()

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)

                # count = data['count']
                # total_count += count
                # print("Consumed record with key {} and value {}, \
                #      and updated total count to {}"
                #      .format(record_key, record_value, data))

                prev_time = int(data['ACT_TIME'])
                valid = validateData(data, prev_time)
                if (valid == 1): # failed an assertion
                    # Output to file
                    failed_writer.writerow(data)
                
                else:
                    bc_dic = {
                        'tstamp': str(data["OPD_DATE"]) + " " + str(convert_time(data["ACT_TIME"])), 
                        'latitude': data['GPS_LATITUDE'],
                        'longitude': data['GPS_LONGITUDE'],
                        'direction': data['DIRECTION'],
                        'speed' : data['VELOCITY'],
                        'trip_id': data['EVENT_NO_TRIP']
                    }
                    trip_dic = {
                        'trip_id': data['EVENT_NO_TRIP'], 
                        'route_id': data['EVENT_NO_STOP'],
                        'vehicle_id': data['VEHICLE_ID'],
                        'service_key': data["OPD_DATE"],
                        'direction': "Out" 
                    }
                    # Output to files
                    breadcrumb_writer.writerow(bc_dic)
                    trip_writer.writerow(trip_dic) 

                # f.write(str(data))
                '''
                for key, value in data:
                    f.write(str(key))
                    f.write(': ')
                    f.write(str(value))
                    f.write(',\n')
                f.write('\n\n')
                '''

    except (KeyboardInterrupt, IOError):
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        breadcrumb_csv.close()
        trip_csv.close()
        failed_csv.close()

        # Load data into the database
        load_data(BC_file, TP_file, False)
