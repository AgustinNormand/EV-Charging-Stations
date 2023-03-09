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

def not_scraped(latitude_longitude, records):
    for record in records:
        if record["coordinates"] == latitude_longitude:
            return False
    return True

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
            if processed_name not in just_websites:
                tuple_latitude_longitude = (result["coordinates"]["latitude"], result["coordinates"]["longitude"])
                if processed_name not in places_near_ev.keys():
                    processed_record = {}
                    processed_record["id"] = get(result, "id")
                    processed_record["alias"] = get(result, "alias")
                    processed_record["name"] = get(result, "name")
                    processed_record["categories"] = get(result, "categories")
                    processed_record["coordinates"] = tuple_latitude_longitude
                    processed_record["phone"] = get(result, "phone")
                    try:
                      processed_record["adress"] = " ".join(result["location"]["display_address"])
                    except Exception as e:
                      processed_record["adress"] = " "
                    processed_record["city"] = result["location"]["city"]
                    processed_record["state"] = result["location"]["state"]
                    processed_record["website"] = get(result, "url")
                    places_near_ev[processed_name] = [processed_record]
                else:
                    if not_scraped(tuple_latitude_longitude, places_near_ev[processed_name]):
                        processed_record = {}
                        processed_record["id"] = get(result, "id")
                        processed_record["alias"] = get(result, "alias")
                        processed_record["name"] = get(result, "name")
                        processed_record["categories"] = get(result, "categories")
                        processed_record["coordinates"] = tuple_latitude_longitude
                        processed_record["phone"] = get(result, "phone")
                        try:
                          processed_record["adress"] = " ".join(result["location"]["display_address"])
                        except Exception as e:
                          processed_record["adress"] = " "
                        processed_record["city"] = result["location"]["city"]
                        processed_record["state"] = result["location"]["state"]
                        processed_record["website"] = get(result, "url")
                        places_near_ev[processed_name].append(processed_record)
    stations_done += 1
    print(stations_done)
    #if stations_done == 1000:
    #  with gzip.GzipFile("./ResultsProcessor_Counter_Memory_1000_Stations_Results", "wb") as f:
    #    pickle.dump(places_near_ev, f)


with gzip.GzipFile("./ResultsProcessor_Counter_All_Info_Results", "wb") as f:
    pickle.dump(places_near_ev, f)
