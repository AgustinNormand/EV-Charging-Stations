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

with open('just_websites.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        just_websites = row

for file_name in results:
    with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            processed_name = result["name"].replace(" ", "").replace("/", "")
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
                        processed_record["website"] = get(result, "url")
                        places_near_ev[processed_name].append(processed_record)
    #break
    stations_done += 1
    print(stations_done)


with gzip.GzipFile("./places_near_ev_all_info", "wb") as f:
    pickle.dump(places_near_ev, f)
"""


for file_name in results:
    with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
        results = pickle.load(f)
        for result in results:
            processed_name = result["name"].replace(" ", "").replace("/", "")
            tuple_latitude_longitude = (result["coordinates"]["latitude"], result["coordinates"]["longitude"])

            if processed_name not in places_near_ev.keys():
                processed_record = {}
                #processed_record["id"] = get(result, "id")
                #processed_record["alias"] = get(result, "alias")
                processed_record["name"] = get(result, "name")
                #processed_record["image_url"] = get(result, "image_url")
                #processed_record["is_closed"] = get(result, "is_closed")
                processed_record["url"] = [get(result, "url")]
                processed_record["review_count"] = [get(result, "review_count")]
                processed_record["categories"] = get(result, "categories")
                processed_record["riting"] = [get(result, "rating")]
                processed_record["coordinates"] = [tuple_latitude_longitude]
                #processed_record["transactions"] = get(result, "transactions")
                processed_record["price"] = [get(result, "price")]
                #processed_record["address1"] = result["location"]["address1"]
                #processed_record["city"] = result["location"]["city"]
                #processed_record["zip_code"] = result["location"]["zip_code"]
                #processed_record["country"] = result["location"]["country"]
                #processed_record["state"] = result["location"]["state"]
                processed_record["phone"] = get(result, "phone")
                processed_record["distance"] = [result["distance"]]
                places_near_ev[processed_name] = processed_record
            else:
                #processed_record = places_near_ev[processed_name]

                # if processed_record["name"] != result["name"]:
                #    print("Warning {} {}".format(processed_record["name"], result["name"]))

                if tuple_latitude_longitude not in places_near_ev[processed_name]["coordinates"]:
                    places_near_ev[processed_name]["coordinates"].append(tuple_latitude_longitude)
                    places_near_ev[processed_name]["distance"].append(result["distance"])
        stations_done += 1
        print(stations_done)
        break

with gzip.GzipFile("./places_coordinates", "wb") as f:
    pickle.dump(places_near_ev, f)

processed_places = {}    
for key in places_near_ev.keys():
  record = places_near_ev[key]
  record["count"] = len(record["coordinates"])
  record.pop("coordinates")
  
  acum = 0
  counter = 0
  for distance in record["distance"]:
    counter += 1
    acum += distance
  record["average_distance"] = acum / counter    
  record.pop("distance")
  processed_places[key] = record
  
with gzip.GzipFile("./processed_places_coordinates", "wb") as f:
    pickle.dump(processed_places, f)
"""
