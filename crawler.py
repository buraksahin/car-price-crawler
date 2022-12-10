"""
Python Multi Threading
JSON Handler
"""
import sys
import requests
import pandas as pd
import numpy as np 
from bs4 import BeautifulSoup
from openpyxl import Workbook
import json
import random
import string
from threading import Thread
from queue import Queue
import time

threadCount = 5
totalData = 128
totalCustomer = 100
totalPost = 5

brands = []
models = []

MAX_PAGE = 50
SEPERATOR = "%2F"
URL_PREFIX = "https://www.arabam.com/ikinci-el/otomobil/"
URL_SUFFIX = "?take=50&page="
URL = "https://www.arabam.com/listing/GetFacets?url=https:%2F%2Fwww.arabam.com%2Fikinci-el%2Fotomobil"
HEADER = {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1)' +
        'AppleWebKit/537.36 (KHTML, like Gecko)' +
        'Chrome/39.0.2171.95 Safari/537.36'}
MESSAGE_CRAWLING_CARS = "crawling models..........[DONE]"
SEND_TO_SERVICE_DONE  = "send to service..........[DONE]"

##
# Handle Brands Data Works
##
class HandleBrandsData():
    ##
    # Initialize HandleJSON object
    ##
    def __init__(self):
        print("Brand Model Handler has been initialized..")

    ##
    # Prepare Cade Brands and Models
    ##
    def prepareBrandsAndModels(self):
         # prepare brands
        resp=requests.get(URL, headers = HEADER)
        brandData = resp.json() # Check the JSON Response Content documentation below
        brandData = brandData['Data']['Facets'][0]['SelectedCategory']['SubCategories']
        for i in brandData:
            brandPath = i['AbsoluteUrl']
            brands.append(brandPath)
            # prepare models
            time.sleep(0.0001)
            respModel = requests.get(URL + SEPERATOR + brandPath.split('/')[1], headers=HEADER)
            modelsData = respModel.json() # Check the JSON Response Content documentation below
            modelsData = modelsData['Data']['Facets'][0]['SelectedCategory']['SubCategories']
            for x in modelsData:
                modelPath = x['AbsoluteUrl']
                print(modelPath)
                models.append(modelPath)

##
# Thread Handler
##
class ThreadHandler(Thread):
    def __init__(self, workQueue):
        Thread.__init__(self)
        self.workQueue = workQueue
        print("Thread initializing..\n")

    def run(self):
        while True:
            try:
                carsModel = []
                carsYear = []
                carsKm = []
                carsColor = []
                carsPrice = []
                carsListingDate = []
                carsProvince = []
                brandSub = self.workQueue.get()
                print(brandSub)
                for i in range(1, MAX_PAGE):
                    isBreaked = False
                    print('+', end = ' ')
                    url = URL_PREFIX + brandSub.split('/')[1] + URL_SUFFIX + str(i)
                    result=requests.get(url, headers=HEADER)
                    time.sleep(0.0001)
                    src=result.content
                    soup = BeautifulSoup(src,'lxml')
                    if "no-result-container" in src.decode('utf-8'):
                        break
                    for x in soup.select('tr td:nth-child(2)'):
                        carModel = x.get_text().strip()
                        if carModel!='Model':
                            carsModel.append(carModel)
                    for x in soup.select('tr td:nth-child(4)'):
                        carYear = x.get_text().strip()
                        if carYear!='Yıl':
                            carsYear.append(carYear)
                    for x in soup.select('tr td:nth-child(5)'):
                        carKm= x.get_text().strip()
                        if carKm!='Kilometre':
                            carsKm.append(carKm)
                    for x in soup.select('tr td:nth-child(6) a'):
                        carColor=x.get_text().strip()
                        if carColor!='Renk':
                            carsColor.append(carColor)
                    for x in soup.select('tr td:nth-child(7) .listing-price'):
                        carPrice=x.get_text().strip()
                        if carPrice!='Fiyat':
                            carsPrice.append(carPrice)
                    for x in soup.select('tr td:nth-child(8) a'):
                        carListingDate=x.get_text().strip()
                        if carListingDate!='Tarih':
                            carsListingDate.append(carListingDate)
                    for x in soup.select('tr td:nth-child(9) a'):
                        carProvince=x.get_text().strip()
                        if carProvince!='İl / İlçe':
                            carsProvince.append(carProvince)
                print(" ")

                # map to struct
                carsData = []
                carModelsData = np.array(carsModel)
                carYearsData = np.array(carsYear)
                carKms = np.array(carsKm)
                carColors = np.array(carsColor)
                carPrices = np.array(carsPrice)
                for i in range(len(carModelsData)):
                    car={'model':carModelsData[i],
                        'year':carYearsData[i],
                        'km':carKms[i],
                        'color':carColors[i],
                        'price':carPrices[i],
                        'listing_date':carsListingDate[i],
                        'listing_province':carsProvince[i].splitlines(0)[0],
                        'listing_distinct':carsProvince[i].splitlines(0)[1]
                    }
                    carsData.append(car)
                response = requests.post("http://localhost:8099/collect/saveBulkRecord", json = {"cars":carsData})
                print(response)
                print(SEND_TO_SERVICE_DONE)
            except Exception as e:
                print(e, "\n")  # Debug
            finally:
                self.workQueue.task_done()
##
# Main Class
##
class Main():
    def __init__(self):
        self.start()

    def start(self):
        # Prepare car models
        jHandle = HandleBrandsData()
        jHandle.prepareBrandsAndModels()

        print(MESSAGE_CRAWLING_CARS)

        # Create work queue
        workQueue = Queue()

        # Put works to queue
        for data in models:
            workQueue.put(data)

        # Create threads
        threads = []
        for i in range(threadCount):
            t = ThreadHandler(workQueue)
            t.daemon = True
            t.start()
            threads.append(t)
        workQueue.join()

"""
Run
"""
startTime = time.time()
m = Main()
elapsedTime = time.time() - startTime
print("Time has been elapsed: ", elapsedTime)
i = input("Press a key for exit..")