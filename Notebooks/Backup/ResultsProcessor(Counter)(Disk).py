#!/usr/bin/env python
# coding: utf-8

import os
import gzip
import pickle
import threading
import queue
import shutil

shutil.rmtree("./workers_partial_files")
os.mkdir("./workers_partial_files")


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
        worker_dir = "./workers_partial_files/worker_{}/".format(self.worker_number)
        os.mkdir(worker_dir)
        while True:
            file_name = self.tasks_queue.get()

            if file_name == "END":
                #with gzip.GzipFile("./worker_results/worker_{}".format(self.worker_number), "wb") as f:
                #  pickle.dump(self.places_near_ev, f)
                break
                
            with gzip.GzipFile("./results/{}".format(file_name), "rb") as f:
                results = pickle.load(f)
                for result in results:
                    need_to_save = False
                    processed_name = result["name"].replace(" ", "").replace("/", "")
                    tuple_latitude_longitude = (result["coordinates"]["latitude"], result["coordinates"]["longitude"])

                    if processed_name not in os.listdir(worker_dir):
                        with open("{}/{}".format(worker_dir, processed_name), "wb") as f2:
                            processed_record = {}
                            processed_record["name"] = result["name"]
                            processed_record["coordinates"] = [tuple_latitude_longitude]
                            pickle.dump(processed_record, f2)
                    else:
                        with open("{}/{}".format(worker_dir, processed_name), "rb") as f2:
                            processed_record = pickle.load(f2)
                            #if processed_record["name"] != result["name"]:
                            #  print("Warning {} {}".format(processed_record["name"], result["name"]))
                            if tuple_latitude_longitude not in processed_record["coordinates"]:
                                processed_record["coordinates"].append(tuple_latitude_longitude)
                                need_to_save = True

                        if need_to_save:
                            with open("{}/{}".format(worker_dir, processed_name), "wb") as f2:
                                pickle.dump(processed_record, f2)

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


