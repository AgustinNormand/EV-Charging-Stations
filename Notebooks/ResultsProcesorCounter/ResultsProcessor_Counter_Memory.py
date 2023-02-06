#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle

stations = {}
with open("coordinates.csv", "r") as f:
    first_line = True
    for line in f.readlines():
        if first_line:
            first_line = False
        else:
            id, latitude, longitude = (line.replace("\n", "").split(","))
            stations[id] = {}
            stations[id]["coordinates"] = [latitude, longitude]

places_near_ev = {}
stations_done = 0
results = os.listdir('./results')

def get(result, key):
    try:
        return result[key]
    except Exception as e:
        return None

for file_name in results:
    with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            processed_name = result["name"].replace(" ", "").replace("/", "")
            tuple_latitude_longitude = (result["coordinates"]["latitude"], result["coordinates"]["longitude"])

            if processed_name not in places_near_ev.keys():
                places_near_ev[processed_name] = [tuple_latitude_longitude]
            else:

                # if processed_record["name"] != result["name"]:
                #    print("Warning {} {}".format(processed_record["name"], result["name"]))

                if tuple_latitude_longitude not in places_near_ev[processed_name]:
                    places_near_ev[processed_name].append(tuple_latitude_longitude)

        stations_done += 1
        print(stations_done)
        break

with gzip.GzipFile("./FirstResults", "wb") as f:
    pickle.dump(places_near_ev, f)

