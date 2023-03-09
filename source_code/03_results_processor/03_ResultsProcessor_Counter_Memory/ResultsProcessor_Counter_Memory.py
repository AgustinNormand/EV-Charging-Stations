#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle
import re

stations = {}
with open("../../01_coordinates/coordinates.csv", "r") as f:
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
results = os.listdir('../../02_yelp/05_results')


for file_name in results:
    with gzip.GzipFile("../../02_yelp/05_results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            tuple_latitude_longitude = (result["coordinates"]["latitude"], result["coordinates"]["longitude"])

            if result["name"] not in places_near_ev.keys():
                places_near_ev[result["name"]] = [tuple_latitude_longitude]

            else:

                if tuple_latitude_longitude not in places_near_ev[result["name"]]:
                    coordinates = places_near_ev[result["name"]]
                    coordinates.append(tuple_latitude_longitude)
                    places_near_ev[result["name"]] = coordinates

        stations_done += 1
        print(stations_done)

with gzip.GzipFile("./ResultsProcessor_Counter_Memory_Results", "wb") as f:
    pickle.dump(places_near_ev, f)
