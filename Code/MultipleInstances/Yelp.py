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
    "Uq3IgQh8Nx0GRLHSAlM2FW80mQMtrMaf3g4eiQf_IHVt5aS3Q24HZ8kYRDgQgQPle2cVfKOXp662JYPTZSXYPGGxy4OlrDg0itCKlW6BzRos8sSbBVI_-ZcR3KvSY3Yx",
    "EyvbybAXv6pgiYCnd5AmdVk4syuOczoMsrFXoiUzJH_CxFpYNtlv9ovek0KY5ruX1CpZttJQXd3ldwP6Ml85CvtzIC5hafOcheBwF--DtE_Y7i260CzF68P75u3SY3Yx",
    "GzY6R0mmSjGd0t1RDH8YzoFWfuC9KVPOmsx-2vEdVtoFYk-pyHKB6wnRIehEs8o6aOefuLuQB_BWvTO6EBscoVoszyZs4-GEHSIv1WCW541h7zosVQObGZaqXhzTY3Yx",
    "xQDzrpCHHMNkOSgNpBQk09hR5z9h0_1d74ijo7jJv1uIKUTpZ63u17bpdezyfMy9tuGch79n2CV83KvG9Y1vTKQ0PFxp37wqL9R3EOlBcaYfxq2lyaWZBT_WcB3TY3Yx",
    "v8sXRp1SjKc6yfHGDj3_pm-u4fDZv3pdc_Pk5pMT9b41QpMW2l6KwFmJZwUZJbJP8EPltCvIx36i1kHfYRxGvE6n2_UWUdGVGvVazXX_hZOJuMrRFwaiaOclVh7TY3Yx",
    "NG2abV-M1855GQavPNA8AgthLOx_pkpAMLuDh9xq5gKiel7p6Nu0x5KpYsXJtmjbsAyrnc35O5Ea-6awqA51IGbH-Pi_Fdi3-V_uzXLT4ykXqZUbOujsCyz7oy3TY3Yx",
    "1aaM8QE2dGkBh10rLEl3r-cZQ-1jw5oCUo4FpmZE08Jsp5CjhLnFvEWEBZNWPuN7Ity0FQhmm8xTBFx3rD9BKnLP_VzrkItkgcL1lju4wNoXZqKjTLLDTTcmaTjTY3Yx",
    "YHqvlL-0I4mB7Qc3tTcpCIvch67KvRqtvjGGQksksz3tfgwNBHFCwG89IqiLozWlcksWVCXDrZAv5zJAeZVyCIVNXGvlE542Ty_L2W-6MrOfpRtdqAmMOQz0mUPTY3Yx",
    "n3HUbcO8roIRjzeI_q_LvSJt8PiAqkaIZceI-K9c0Fi4dsu5YxT1HA293m00R272TfjCkyBmgrcI4nBu6RyRtG8yH4aSjhRIN9KRGmq2FxxUWuCz-zC0P1-4nkTTY3Yx",
    "-O271cK2a3EXfOyXkkwmG0oUU0YJYeDv_IWWKkJcuGDmX-sPnx2dlrNaaGEhmzZlYSczC0kJiFt9bW988RP6vc8YDljwdqRwt5x44F02ARcYkhWjhqSKr4Bo6fjTY3Yx",
    "GAKIY_uDhDnOsz4gD9aFrfNzebVaQxeGNN4cP2KPtd3MUsGBgDyGie_aJNUcHEdxN4t-1_bD5Y1AVPTIzbdMxesIagKonE5TRTprFnglmMQtFIAB5ZArHHxxafnTY3Yx",
    "HCDQ0qH5iGQ9Kz6pT4-0pRTXQUHfH7LjVzE6xn_9Ye1EZJIWxoVYi1ETZXS9UuIoKtcobeoIE3VPrG5XoQilwvKpJeNEY6NDoV2CUT-SmPVQ03iFXDCCMh1q1_nTY3Yx",
    "PIH5MbDMUksyIXCnxHV9JAyhJo_BR7y4sJcIVaZAM_qBOHTkToprUmPF5R9VHIUuV5y9mhKBrT-7_ZiA68QLpugkHakQAfcO43jGCQ7QVfHMai1iGp94zHjqwOzSY3Yx",
    "xipEFlyzdJ2uFuL2rhyF5FgBloJPTpMbgVx9BEsVcyXyxXH2EOH6Z2rr8X-VyJGSHcYTni1WtjY3YbYuUbTUABqYZTTgxZ-NWtjqWdlzOKdTnmfUl7J1ykjZ2PvTY3Yx",
    "SPR3eT9yimECeN5b3mII2JLS66DU8jRmF2RHSyqwbF2t9b4vfRDEFdE5RQVcmDGROis43H_ip3Zp7wYnHPrd4-9phGvo3QzU0J6LR7bmlCWqh87fC5f9hkBEgvzTY3Yx",
    "uRP_S5U0vfN30yIVP7EVWXOHhUQNuvKGYrp-NMMdf3LkIld3ta2V_haIsHP0oeoZM2PWX-VP29k3dikwi9szfcRYfkMYHY-nCbRCtCvIjd_EU2LJTwtFTMOL2_3TY3Yx",
    "EXtWG_HWX3FCujJoBlrbdCZ-VZWCZZ_JrsIXI3xa_iBObEEoWVggljoFZvLfPny0Fq1CyjFrgSAunQSaR-oVgnDfdCdjd1swbulGwNPCsZEHI-NeXF0WrUhdt_3TY3Yx",
    "6FFyeQqf7i4reMZskevs0JGdf4kxnuhvNmaYhKeHUI65ggABe7nm9v4cjtRTQ1vWZX1cSkiDc7yoZiXWETjZPLcGH-FIMqt8sHndvCAQ1VqlmeY5axmhl4VKQv7TY3Yx",
    "EHRs7OoP4hsmF-n3Nx8rOF0_sN1JYJ_UXfzXqBPsMtEGG1BI0HirYdNPoq40GzquYDy-X4tZS9872cgbZbMTl554NPiEkao9VlkifJvJ2MT-B5GqOa3lt7uJmP7TY3Yx",
    "ncSqV1_HijiwzqbExeaYnUbp_F1QBl0DQ1XfhgY0nLVeia3-rFSot2B4zq6YKjpnNhR5-uy7eU-rHLKg0GkCe4sx9rYZ-WUlgL1en4hO5N_qLe-SyMrW0qeq-P7TY3Yx",
    "vVjUhmjivf2LbCETJpMNNN2UcJY6sPpuaoQjtOCr-uqTOSbAbUddOv7hk-521WARAoCOme8MPRXcS2IrymBcni66g_bgclUSA6AFluGIJU8Qd7Q5n1D8Jtm3vRrUY3Yx",
    "b0A1iGv-h1NkOp4_ukPyzCmBKP7FAWfOcP6aCYwfFKi4L1XUHZ2viF96fs1B7OVVJHcnFMITZFShGPWDU47g34hwzVIG7tar-9VFiToZFjabH7X8lTqJKzhR_xrUY3Yx",
    "BnD537YXkw_r_0ydKFE24TZSIiI9FO_lKaikqazat-z8fzOowuShJmLzyvsJQoe3_OHYF3SUhfAVRjE8I9gQIx-CAAEvYy94rCWbmZnQaNAp4CHEAPLHq98yRBvUY3Yx",
    "hdA0iDWkPwLi2w0rmxf6F6s2zV5wjTasnd6K7C6b6-1gIkkSXMhEToD8m47YyTTRAQIZrrTARaK3bAY8Lw5VIorP9fhQPQaDHdT0gyAQdJsS44FdfgtoI40vIj_UY3Yx",
    "hdsdYYYC0o3W4vmEHl1FnT2x3vdso9Lo50dlT5doGmzpuma5CuXFgheYzKxB0Jd2rOky1SgEYXniClTNKX5p3Da6ED-1a61ed193dvca7-_-BRRwUTlW_9mOvz_UY3Yx",
    "6ZSklTBhvmC-FYn0fPemJa0dDKd-TGaonOuzT0jyDYt47UoBjpfihxP8N63f2IzpivZu34OAC6AR1E-U0fwiXSz34becFg7yRDZ9icj8mC7jw2wAKw6h1LKNVkHUY3Yx",
    "C9OV_eLxjtwRTj6BA220wXFHY19BvP_FT4f5GPVZiYd_C6OPjhdal8a65c_SW1lYzPcfGGxXTo98TSS_-qeoq4IsoZiBUxn7b9fsiMhwVI0ic4SfH-KHJ4aw_SPVY3Yx",
    "muOoRobkZQVso6WeZXHPCAYc-w-QF0EkZUMrOgXP4ubvGDTDcbgL0U5s2gOTxN3-LydIaa1xxXKkR3K0m71RfPimo32k7rJ2eqA31g0MsIX4z_NpxTuaEpv5EyXVY3Yx",
    "SNpWA1lqenSqOEbDkaStVX2W1CEiGfF-Qsl3n0UtVzeQfvJ7yk-nOqOn2rZyDGuqmZOZttQU9AIokbJSdN1AtpAwD2EkVsWG4PXYuDov3z3iHCJCNgkvFz1MrCXVY3Yx",
    "2iMYOjj01Ho5Oru9N1SUs6lZbcycQ-Te0s-69ces2_NTkmwpqqc1Jpf-lSy67RiEePeACuG_PFeOVROitinDFK1FvEnhPm2ULEF9RwdwqTClO5b4yrzM9e4yXybVY3Yx",
    "cQH3ujxtHg3BMNziw4B8sko815GLWoZF8hZ7zUVrIYa-D77DZnzsG07DmCRNnPf_hd2hw2oMVRFeh-3cOq9SGDtRuPL_YgBCL-w-mYmwsGgnkBu8otog5B1JuijVY3Yx",
    "Q7TJxSNRNQPu8OXMLPI6UK3RL7OIWWp3E00rXZz4MVw7maqWYr9bnOj3Wl2rwYmBLE0XleFaYQ_06DLLZu5TYor51ylQVBBWLATahERBlRu8m_HoJQMMrPrw7ynVY3Yx",
    "0SJx2n35fY5vaYEqb1IAScEeWyJ5zs68FDPE8TQW_FcqEvFhpgqDzGFnnwqNqbqPg47bWsovkqD36CK1g05J7T2sCwZaFmSfJsnoGhLoGdS0fIppsZ-YXTx2vyrVY3Yx",
    "ifd5eaouA_jn3uiSZpMUFeT5hLsDQYYgGjQnm0G5zvNjA25AZnJWoP_dTxNH5r3siTMbl3k5lp1XnsZaPgpTKDUITETz51ErR55gwFyMHZNiMGXLPTUaoZAuTCzVY3Yx",
    "Pmx4sU4x9ejSt-ldqYWXFn3FATKVXRO3VHUkw7DMA_Qo3WgSv9N0Nu9srXtltN2lTdw1jzA-sAZ7SPAyiuSdY57V5_56CYzMjBarP3Vb37KQdUdQYbkbQnulwyvVY3Yx",
    "ydNe5hwFBXzFqEl1VDR0x4MgvQIsyxWGxbV1xyfuOeJVc4V-v-Fq0JZ4GlEFCezJEqcYRF82Q1Tp8JBj9MeXb7nlpfQ4qjKacxci4pTNbkt4uc4WlgbBZZXQ1izVY3Yx",
    "6k-yuIL5MmSJDzaxPnXhsTgS4RmdS3ck5vgI5w6kvu0JGU-hBsniXZs2a3PJVJ41Vtlqh-CfwyeGVg2KuaEl3QSi_w2KsbkLGp6T8xohwDravykAnpePN_0OTi3VY3Yx",
    "tTs7CSOBonnovXhm6MV4EPSym1hb1JGYeS6Ys_Zxx_G_QD-fi0IM9t2Pvhg9H-zUNbsv-7VKnRAHptIh4t5uDlaExBoCWe4mjJohCe1alY6GyseA5qGh9EAy7C3VY3Yx",
    "_49BY5wBaNoPSfHSGLaxpc_aVldaU9tzrEwHaQ7if_uSkJhHC2ha4RxYf8pBhVK3LO4tx9TA3zML7P0_sfcbSoBqvVGq5c4pFKfLow1fnozaynQwPNqJ0MMDCi_VY3Yx",
    "VkdFnJaCEmdN1CImv5UP5C08-LytZBrRnNEQKTatCGfJWk9_nw46l5xW3O6EUOxh6SSR1ptZTTmxAKn6vNKukeYVdJD8teRan_T2AzQWcoGluNBmftJaA5c_yy_VY3Yx",
    "pOvghnK4bZzPplTDmxFHtAlOgnAncuebK9uPTB_ZLGTBl3YUqNkeOYXwW1ICV_VyFNxY2Dz8Z7NDXjjTDWBU8YGQ4lvM2tZjUtWFM4mCjvDCPnZ4tBrMNHnyejDVY3Yx",
    "HUrElgKlr_aJXunu8umzhFV7-sUad_HxHdIWG9BsLNp1OUkL0Cesyji8glF7S4N2CdQ8NqMvRRAs2SFQ3tKp0diO_Ka15s-9ICYHdiKb6mb6o5EsQHMfE4BSMTHVY3Yx",
    "_ozoxLDBd_8AQ32FQRllJnj3MGT4FLZx-XaGkwQ1kBnomTV3BVUrVjT6ZKt8KOxRmH0xBzB5hYXzG4bQPCxuI6eqMIgcRiOwMaLaQzEdDYmughEF07Pc5-F6rDLVY3Yx",
    "W94nzRw-AIF9dlYO41Fx6siJXyRnS-72AEhRMd87dArfkaRhkA8f0gvkGw5oUTZBEO3GzxerJXGpPTDQGDGmYW8Bbwe0qmb76NS1C4wsnYbTGaSbxLlcsxa7OfzVY3Yx",
    "xUXxW0K9q51wSWWB9cSvVTVlCDrv5CAGB7KCmwTdsueuziECl9o9sBwf8Kb46w_-O4JwKKWWrFG1-u1zJuiiAKPmS1Rm1RgMHJzBdRquk0zbhGjJqQLTIpcpYJ_WY3Yx",
    "gcwlj0JEdsy6M1syDxrWu9jSMLzutN3Y0rdK7W8C5bMnN_vpv3Bk680a1_XksUZ84TRorjc4kpLbQ1-j57FDtJbgy1XwOjGxe9DcEah1k7vuueAK-iQzawGADKDWY3Yx",
    "vPulPED1ge5O5X5ZtaeKGmhM-Vbl_nricdfIpjeBFmtUZ7SdkSasBJ8T5i1f31zTDjhCfBx-jrjc7-XyUpuG4Z1cjw8FrS8mpUZwQKzQvLiT2FmkLxmLlabR86nWY3Yx",
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

logging.basicConfig(filename='./log/application.log', level=logging.INFO, filemode="a", force=True)

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

already_scraped = os.listdir('./results')

already_scraped_processed = []
for result in already_scraped:
    already_scraped_processed.append(int(result.split("_")[1]))

for station_id in missing_to_scrape_worker:
    station_id = station_id.replace("\"", "")
    if int(station_id) not in already_scraped:
        tasks_queue.put(station_id)

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
