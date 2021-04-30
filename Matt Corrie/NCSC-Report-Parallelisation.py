import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

import re
import pandas as pd
import numpy as np
from IPython.display import clear_output

from functools import reduce
from multiprocessing import Pool, cpu_count, current_process
import datetime

# creating the function to get title, articles and tags

def get_articles(link):

    chrome_options = Options()
    chrome_options.add_argument("headless")
    chrome_options.add_argument("no-sandbox")
    chrome_options.add_argument("disable-gpu")
    gbrowser = webdriver.Chrome(executable_path='chromedriver.exe',chrome_options=chrome_options)
    gbrowser.get(link)
    WebDriverWait(gbrowser, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "pcf-title")))

    articles = []
    t = []

    try:

        title = gbrowser.find_elements_by_class_name("pcf-title")
        try:
            title = title.text
        except:
            title = title[0].text

        main = gbrowser.find_elements_by_class_name('pcf-BodyText')
        try:
            articles = main.text
        except AttributeError:
            for i in range(len(main)):
                articles.append(main[i].text)

        topics = gbrowser.find_elements_by_class_name('topic-tags-container')
        topics = topics[0].text[7:]
        count = len(re.findall('([A-Z])', topics))
        for num1, i in enumerate(re.finditer('([A-Z])',topics)):
            for num2, j in enumerate(re.finditer('([A-Z])',topics)):
                if num1 == num2 + 1:
                    t.append(topics[j.start():i.end()-1])
                elif num1 == num2 & num1 == count - 1:
                    t.append(topics[i.start():])

    except:
        pass

    data = {'Title': title,'Article': articles,'topics': t}
    gbrowser.close()

    return data

def get_links():

        # setting options
        chrome_options = Options()
        chrome_options.add_argument("headless")
        chrome_options.add_argument("no-sandbox")
        chrome_options.add_argument("disable-gpu")

        # getting links to be looped over
        linkbrowser = webdriver.Chrome(executable_path='chromedriver.exe',chrome_options = chrome_options)
        linkbrowser.get('https://www.ncsc.gov.uk/section/keep-up-to-date/threat-reports?q=&defaultTypes=report&sort=date%2Bdesc&writtenFor=Large+organisations&rows=232')

        links=[]

        if links == []:
            WebDriverWait(linkbrowser, 10).until(EC.presence_of_element_located((By.XPATH, ".//a")))

            for a in linkbrowser.find_elements_by_xpath('.//a'):
                links.append(a.get_attribute('href'))
        else:
            linkbrowser.close()

        report_links = []
        for ind, link in enumerate(links):
            if 'report/weekly' in link:
                report_links.append(link)

        return report_links

# creating our multi-processing

if __name__ == "__main__":

    begin_time = datetime.datetime.now()
    # print the number of cores
    print("Number of cores available equals %s" % cpu_count())
    print("Using %s cores" % 16)

    # create a pool of workers
    # start all worker processes
    pool = Pool(processes= cpu_count())

    with pool as p:
        report_links = p.map(get_links)

    pool = Pool(processes= cpu_count())

    # context manager so no need to close
    with pool as p:
        results = p.map(get_articles, [link for link in report_links])

    print("Finished getting articles!")
    end_time = datetime.datetime.now()
    print(end_time - begin_time)
