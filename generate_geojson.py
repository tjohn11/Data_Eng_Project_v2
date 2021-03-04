#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
from db_to_tsv import generate_tsv
features = []
tsv_filename = '/home/shared_dir/assignment_1/examples/clients/cloud/python/heatmap_data.tsv'
geojson_filename = '/home/shared_dir/assignment_1/examples/clients/cloud/python/heatmap_data.geojson'

generate_tsv(tsv_filename)

with open(tsv_filename, newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()
    for line in data[1:]:
        row = line.split("\t")
        
        # Uncomment these lines
        lat = row[0]
        longi = row[1]
        speed = row[2]

        # skip the rows where speed is missing
        if speed is None or speed == "":
            continue
     	
        try:
            latitude, longitude = map(float, (lat, longi))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(speed))
                    }
                )
            )
        except ValueError:
            continue

collection = FeatureCollection(features)
with open(geojson_filename, "w") as f:
    f.write('%s' % collection)
