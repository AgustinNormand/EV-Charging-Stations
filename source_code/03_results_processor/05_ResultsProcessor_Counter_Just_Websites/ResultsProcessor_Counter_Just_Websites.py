#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle
import csv

def get(result, key):
    try:
        return result[key]
    except Exception as e:
        return None

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

with open('../03_ResultsProcessor_Counter_Memory/just_websites.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        just_websites = row

for file_name in results:
    with gzip.GzipFile("../../02_yelp/05_results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            processed_name = result["name"].replace(" ", "").replace("/", "").replace(",", "")
            if processed_name in just_websites:
                if processed_name not in places_near_ev.keys():
                    processed_record = {}
                    processed_record["name"] = get(result, "name")
                    processed_record["one_website"] = get(result, "url")
                    processed_record["categories"] = get(result, "categories")
                    places_near_ev[processed_name] = processed_record

    #if len(places_near_ev.keys()) == 2072:
    #  break
      
    stations_done += 1
    print(stations_done)
    #if stations_done == 1000:
    #  with gzip.GzipFile("./places_near_ev_just_websites_demo", "wb") as f:
    #    pickle.dump(places_near_ev, f)


with gzip.GzipFile("./ResultsProcessor_Counter_Just_Websites_Results", "wb") as f:
    pickle.dump(places_near_ev, f)
