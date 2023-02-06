#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle
import threading
import queue

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

class ProcessorWorker(threading.Thread):
    def __init__(self, worker_number, stations, tasks_queue):
        threading.Thread.__init__(self)
        self.worker_number = worker_number
        self.stations = stations
        self.tasks_queue = tasks_queue
        self.places_near_ev = {}
        
    def run(self):
        os.mkdir("./workers_partial_files/worker_{}/".format(self.worker_number))
        """
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
                        self.places_near_ev[result["name"]] = [result["location"]]
                    else:
                        if result["location"] not in self.places_near_ev[result["name"]]:
                            self.places_near_ev[result["name"]].append(result["location"])
        """
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


