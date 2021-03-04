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
from load_insert import load_event_data

event_csv_file = 'event_data.csv'
failed_event_file = 'failed_event_data.csv'

def validateData(data):
    # Validations
    try:
        assert(data['trip_id'] is not '')
    except AssertionError:
        print('***TRIP ID DNE EXCEPTION***')
        return 1
    try:
        assert(data['vehicle_number'] is not '')
    except AssertionError:
        print('***VEHICLE NUMBER DNE EXCEPTION AT TRIP ID: %s ***' % data['trip_id'])
        return 1
    try:
        valid_direction = ['0', '1']
        assert(data['direction'] in valid_direction)
    except AssertionError:
        print('***INVALID DIRECTION EXCEPTION AT TRIP ID: %s ***' % data['trip_id'])
        return 1
    try:
        valid_service_key = ['W', 'S', 'U']
        assert(data['service_key'] in valid_service_key)
    except AssertionError:
        print('***INVALID SERVICE KEY EXCEPTION AT TRIP ID: %s ***' % data['trip_id'])
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

    # Process messages
    event_csv = open(event_csv_file, 'w')
    failed_csv = open(failed_event_file, 'w')

    event_headers = [
        'trip_id',
        'vehicle_number',
        'route_number',
        'direction',
        'service_key'
    ]    
    failed_headers = [
        'trip_id',
        'vehicle_number',
        'leave_time',
        'train',
        'route_number',
        'direction',
        'service_key',
        'stop_time',
        'arrive_time',
        'dwell',
        'location_id',
        'door',
        'lift',
        'ons',
        'offs',
        'estimated_load',
        'maximum_speed',
        'train_mileage',
        'pattern_distance',
        'location_distance',
        'x_coordinate',
        'y_coordinate',
        'data_source',
        'schedule_status'
    ]

    try:
        event_data_writer = csv.DictWriter(event_csv, fieldnames=event_headers)
        event_data_writer.writeheader()
        failed_data_writer = csv.DictWriter(failed_csv, fieldnames=failed_headers)
        failed_data_writer.writeheader()

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

                valid = validateData(data)

                if (valid == 1): # failed an assertion
                    # Output to failed data file
                    failed_data_writer.writerow(data)
                
                else:
                    # Output to valid data file
                    to_write_data = {
                        'trip_id': data['trip_id'],
                        'vehicle_number': data['vehicle_number'],
                        'route_number': data['route_number'],
                        'direction': data['direction'],
                        'service_key': data['service_key'],
                    }
                    event_data_writer.writerow(to_write_data)
                    print(to_write_data)

    except (KeyboardInterrupt, IOError):
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
        event_csv.close()
        failed_csv.close()

        # Load data into the database
        load_event_data(event_csv_file)

