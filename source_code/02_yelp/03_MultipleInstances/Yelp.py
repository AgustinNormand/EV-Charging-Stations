#!/usr/bin/env python

import requests
import json
import queue
import time
import threading
import pickle
import gzip
import logging
from datetime import datetime
from requests.adapters import HTTPAdapter, Retry
import sys
import os

API_KEYS = [
    "qGi5Av_ucRHmQLWE7IZfBRx-utuRenX-4VkQDqCAkVLHBUmbmVjbDuTLiBj9b4XGO4ju8EE4SRa6Yyev0qGlqiJ0v4pFT-eALMc8yQ-qXHxPo_b6YfiaUVSDo2reY3Yx",
    "sme8uoEbjINbc8QODPKbUymCg6oghxSTVbInCoWH2BDyR04npgNDhkLx6ZSwLJaLWKdCLGAvkTK0lZZSYY7OHnHXUpWymgI3vr9FJApZdYpWqWxlNO6lZpHRMWveY3Yx",
    "wwUn1liDfLX_MUxQ6evOlJXgNVL9xS3C_4rMQY8yK4GqMsKCsGltiTGOlq1w4LwgoJNIapeYraD2HhwJvy9Q-O0DdsZT6f8uSJjsEjyn0uAXIhIwCBhF2iWvpmveY3Yx",
    "uMenApaT3CAtYiNFjzH2b7bnMil3oO1mQUK6-y3S5NizvVZxaeBPhr9yyraDvkleyjaSQXlx5aG63fYe1VdnysLoJLuqcYiLx6prG31I3ctyirMyWSyj-3lCVmzeY3Yx",
    "peF5yOMqMszsE4z8D9qNS0QcdMINN1AO7WWauqQCNvc5pEM6-rTPIzavsDWpn4aVcItrkkm5j4sJ3WB2JE-1AVNw4kaHohAXn_di5MfS_d0lCwzjSXbFenGf1mzeY3Yx",
    "JQ5VwZomkp0RZ8KkJXHt0rWiOglvXfUl4lxRe9eLeZy2GurlgZ3Yiorr_xeSMDurKuEnHS5vlm-DqxWDTRkxF6thMSjxFiEShIAMQpWqvXOQ7ZYdigdPtY2OV23eY3Yx",
    "foLImsRGsChpcAe5QiBhI4xTOF3lFuvphUzk8kWDb4nWy2Bv_JL1D6Fvizx1atBpyD1geOpVIRS8HViOwB22xDgbC1NJoH1CGvFgtkkU6PJUrM_Lt7aF4iqGHG7eY3Yx",
]

yelp_categories = [
    "coffeeshops",
    "food",
    "restaurants",
    "movietheaters",
    "shopping",
    "souvenirs",
    "arcades",
    "museums",
    "barbers",
    "hair",
    "othersalons",
    "massage",
    "casinos",
    "sportgoods"
]

workers = int(sys.argv[1]) #Cantidad de workers realizando peticiones
worker_number = int(sys.argv[2]) #Numero de worker que esta ejecutando este código

logging.basicConfig(filename='./log/application.log', level=logging.INFO, filemode="w", force=True)

stationsIds_coordinates = {}
with open("coordinates.csv", "r") as f:
    first_line = True
    for line in f.readlines():
        if first_line:
            first_line = False
        else:
            id, latitude, longitude = (line.replace("\n", "").split(","))
            stationsIds_coordinates[id] = [latitude, longitude]

missing_to_scrape = []
with open("missing_to_scrape.csv", "r") as f:
    for line in f.readlines():
        for value in line.split(","):
            missing_to_scrape.append(value)

# Divido los missing to download
logging.info("{}: Worker number arranca desde 0".format(datetime.now()))
logging.info("{}: Workers: {}, Worker Number: {}".format(datetime.now(), workers, worker_number))
logging.info("{}: Missing to download count {}".format(datetime.now(), len(missing_to_scrape)))

worker_size = len(missing_to_scrape) // workers
start_interval = 0 + ((worker_size)*worker_number)
finish_interval = start_interval + worker_size

logging.info("{}: Worker interval Start: {}, Finish {}".format(datetime.now(), start_interval, finish_interval))

if (worker_number + 1) == workers:
  # Es el ultimo worker, agarro desde el start_interval hasta el final
  missing_to_scrape_worker = missing_to_scrape[start_interval:]
else:
  missing_to_scrape_worker = missing_to_scrape[start_interval:finish_interval]

logging.info("{}: Missing_to_scrape Worker Len {}".format(datetime.now(), len(missing_to_scrape_worker)))

tasks_queue = queue.Queue()

for station_id in missing_to_scrape_worker:
    tasks_queue.put(station_id.replace("\"", "").replace("\n", ""))

logging.info("{}: Q Len {}".format(datetime.now(), tasks_queue.qsize()))

# Divido las api keys
logging.info("{}: Worker number arranca desde 0".format(datetime.now()))
logging.info("{}: Workers: {}, Worker Number: {}".format(datetime.now(), workers, worker_number))
logging.info("{}: Api Keys count {}".format(datetime.now(), len(API_KEYS)))

worker_size = len(API_KEYS) // workers
start_interval = 0 + ((worker_size)*worker_number)
finish_interval = start_interval + worker_size

logging.info("{}: Worker interval Start: {}, Finish {}".format(datetime.now(), start_interval, finish_interval))

if (worker_number + 1) == workers:
  # Es el ultimo worker, agarro desde el start_interval hasta el final
  API_KEYS_worker = API_KEYS[start_interval:]
else:
  API_KEYS_worker = API_KEYS[start_interval:finish_interval]

logging.info("{}: API_KEYS_worker Worker Len {}".format(datetime.now(), len(API_KEYS_worker)))

def build_yelp_url(latitude, longitude, category=None):
    yelp_url = None
    sort_by = "distance"
    radius = 40000

    if category == None:
        or_str_categories = ""
        for category in yelp_categories:
            or_str_categories += str(category) + ","

        yelp_url = "https://api.yelp.com/v3/businesses/search" \
                   "?radius={}" \
                   "&latitude={}" \
                   "&longitude={}" \
                   "&categories={}" \
                   "&sort_by={}".format(radius, latitude, longitude, or_str_categories[:-1], sort_by)
    return yelp_url


class StationsDownloaderWorker(threading.Thread):
    def __init__(self, worker_number, tasks_queue, stationsIds_coordinates, api_key):
        threading.Thread.__init__(self)
        self.worker_number = worker_number
        self.tasks_queue = tasks_queue
        self.stationsIds_coordinates = stationsIds_coordinates
        self.requests_before_sleep = 0
        logging.info("{}: Starting worker {}, with api key {}".format(datetime.now(), self.worker_number, api_key))
        self.headers = {
            "accept": "application/json",
            "Authorization": "Bearer {}".format(api_key)
        }

        self.s = requests.Session()

        retries = Retry(total=20,
                        backoff_factor=60,
                        status_forcelist=[500, 502, 503, 504])

        adapter = HTTPAdapter(max_retries=retries)

        self.s.mount('http://', adapter)
        self.s.mount('https://', adapter)

    def quota_exceded(self, response):
        return "ACCESS_LIMIT_REACHED" in response.text

    def request(self, yelp_url_with_limit_and_offset):
        try:
            response = self.s.get(yelp_url_with_limit_and_offset, headers=self.headers)
            self.requests_before_sleep += 1

            if self.quota_exceded(response):
                logging.info(
                    "{}: Worker Number {}, Response Status Code {}, Response Text {}, yelp_url {}, Aprox Requests Done {}, Sleeping 24 hours".format(
                        datetime.now(), self.worker_number, response.status_code, response.text, yelp_url_with_limit_and_offset, self.requests_before_sleep))
                self.requests_before_sleep = 0
                time.sleep(86400)

                response = self.s.get(yelp_url_with_limit_and_offset, headers=self.headers)
                self.requests_before_sleep += 1

            json_response = json.loads(response.text)

            return json_response

        except Exception as e:
            logging.error("{}: Error in Worker Number {}, Exception Text {}, yelp_url {}".format(
                        datetime.now(), self.worker_number, str(e),
                        yelp_url_with_limit_and_offset))
            return None

    def get_all_results(self, yelp_url):
        results = []
        for i in range(0, 20):
            offset = i * 50
            yelp_url_with_limit_and_offset = "{}&limit={}&offset={}".format(yelp_url, 50, offset)

            json_response = self.request(yelp_url_with_limit_and_offset)

            logging.info("{}: Worker Number {}, Total for resource {}, offset {}, Total in response {}".format(datetime.now(), self.worker_number,
                                                                                                    json_response[
                                                                                                        "total"],
                                                                                                    offset, len(
                    json_response["businesses"])))
            results.extend(json_response["businesses"])

            if len(json_response["businesses"]) < 50:
                break

        return results

    def run(self):
        while True:
            station_id = self.tasks_queue.get()

            if station_id == "END":
                break
            try:
                latitude, longitude = self.stationsIds_coordinates[station_id]
                logging.info("{}: Worker Number {}, ID {}, Latitude {}, Longitude {}".format(datetime.now(), self.worker_number, station_id,
                                                                                  latitude, longitude))

                yelp_url = build_yelp_url(latitude, longitude)
                results = self.get_all_results(yelp_url)

                with gzip.GzipFile('./results/station_{}_results'.format(station_id), 'wb') as f:
                    pickle.dump(results, f)

            except Exception as e:
                logging.error("{}: Worker Number {}, Error in {}, {}".format(datetime.now(), self.worker_number, station_id, str(e)))

            self.tasks_queue.task_done()




for api_key in API_KEYS_worker:
    tasks_queue.put("END")

workers = []

worker_number = 0
for api_key in API_KEYS_worker:
    worker = StationsDownloaderWorker(worker_number, tasks_queue, stationsIds_coordinates, api_key)
    workers.append(worker)
    worker.start()
    worker_number += 1

for worker in workers:
    worker.join()
