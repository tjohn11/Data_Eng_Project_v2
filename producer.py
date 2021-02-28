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
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import requests
import urllib.request

url = 'http://rbi.ddns.net/getBreadCrumbData'
datafile = '/home/shared_dir/assignment_1/examples/clients/cloud/python/datafile.json'
urllib.request.urlretrieve(url,datafile) 

slack_uri = 'https://hooks.slack.com/services/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'


delivered_records = 0
# # # Optional per-message on_delivery handler (triggered by poll() or flush())
# # # when a message has been successfully delivered or
# # # permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

def test_site(slack_uri):
    success_dict = { "text": "data updated" }
    success_json = json.dumps(success_dict)
    headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
    requests.post(slack_uri, data=success_json, headers=headers)

if __name__ == '__main__':

    # # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    #data_file = args.data_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # # # Create topic if needed
    ccloud_lib.create_topic(conf, topic)


    with open('/home/shared_dir/assignment_1/examples/clients/cloud/python/datafile.json') as f:
        data = json.load(f)

        crumb_count = 0
        for record in data:
            # if crumb_count > 1000:
            #     break
            record_key = 'breadcrumb'
            record_value = record
            json_data = json.dumps(record_value)
            # print("Producing record: {}\t{}\t{}".format(crumb_count, record_key, record_value))
            producer.produce(topic, key=record_key, value=json_data, on_delivery=acked)
            # p.poll() serves delivery reports (on_delivery)
            # from previous produce() calls.
            producer.poll(0)
            crumb_count += 1
            
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

    test_site(slack_uri)

