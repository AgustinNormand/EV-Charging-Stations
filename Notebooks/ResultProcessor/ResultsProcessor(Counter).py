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

import threading
import queue

class ProcessorWorker(threading.Thread):
    def __init__(self, worker_number, stations, tasks_queue):
        threading.Thread.__init__(self)
        self.worker_number = worker_number
        self.stations = stations
        self.tasks_queue = tasks_queue
        self.places_near_ev = {}
        
    def run(self):
        while True:
            file_name = self.tasks_queue.get()

            if file_name == "END":
                with gzip.GzipFile("./worker_results/worker_{}".format(self.worker_number), "wb") as f:
                  pickle.dump(self.places_near_ev, f)
                break
                
            with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
                results = pickle.load(f)
                for result in results:
                    if result["name"] not in self.places_near_ev.keys():
                        self.places_near_ev[result["name"]] = 0
                    self.places_near_ev[result["name"]] += 1
            #break #To only process one

NUMBER_OF_WORKERS = 100

results = os.listdir('./results')

tasks_queue = queue.Queue()

for file_name in results:
    tasks_queue.put(file_name)
    
for i in range(NUMBER_OF_WORKERS):
    tasks_queue.put("END")
    
workers = []
for i in range(NUMBER_OF_WORKERS):
    worker = ProcessorWorker(i, stations, tasks_queue)
    workers.append(worker)
    worker.start()
    
for worker in workers:
    worker.join()


"""
results = os.listdir('./results')

for file in results:
    station_id = file.split("_")[1]
    with gzip.GzipFile("./results/{}".format(file), "rb") as f:
        processed_results = []
        results = pickle.load(f)
        for result in results:
            processed_result = {}
            processed_result["id"] = result["id"]
            processed_result["alias"] = result["alias"]
            processed_result["name"] = result["name"]
            #image_url
            #is_closed
            processed_result["url"] = result["url"]
            #review count
            processed_result["categories"] = result["categories"]
            #raiting
            #coordinates
            #transactions
            #price
            #location
            processed_result["phone"] = result["phone"]
            #display phone
            processed_result["distance"] = result["distance"]
            processed_results.append(processed_result)
            #break
        stations[station_id]["near_places"] = processed_results
    #break

"""

