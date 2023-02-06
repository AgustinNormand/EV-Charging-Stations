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

stations_done = 0
results = os.listdir('./results')


starbucks_coordinates = []
starbucks = []
for file_name in results:
    with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            if result["name"] == "Starbucks":
                if result["coordinates"] not in starbucks_coordinates:
                    starbucks_coordinates.append(result["coordinates"])
                    starbucks.append(result)


"""
for file_name in results:
    with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        just_open = True
        with open("Starbucks.csv", "w") as f:
            for result in results:
                if result["name"] == "Starbucks":
                    if just_open:
                        f.write(",".join(list(result.keys())))
                        f.write("\r\n")
                        just_open = False
                    values = []
                    for value in list(result.values()):
                        values.append(str(value).replace(",",""))
                    f.write(",".join(values))
                    f.write("\r\n")

    break
"""


