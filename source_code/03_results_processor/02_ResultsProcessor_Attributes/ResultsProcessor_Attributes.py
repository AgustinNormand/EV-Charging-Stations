#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle

stations = {}
with open("/home/agustin/PycharmProjects/Stations/source_code/01_coordinates/coordinates.csv", "r") as f:
    first_line = True
    for line in f.readlines():
        if first_line:
            first_line = False
        else:
            id, latitude, longitude = (line.replace("\n", "").split(","))
            stations[id] = {}
            stations[id]["coordinates"] = [latitude, longitude]

stations_done = 0
results = os.listdir('/home/agustin/PycharmProjects/Stations/source_code/02_yelp/05_results')

unique_keys = []
for file_name in results:
    with gzip.GzipFile("/home/agustin/PycharmProjects/Stations/source_code/02_yelp/05_results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            for key in list(result.keys()):
                if key not in unique_keys:
                    unique_keys.append(key)
print(unique_keys)
