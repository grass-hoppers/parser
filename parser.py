import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')

import numpy as np
import urllib3
from tqdm import tqdm
import csv
from bs4 import BeautifulSoup
from selenium import webdriver 
import os
from selenium.webdriver.common.keys import Keys
import time
import datetime
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains

import bs4
import requests
import pandas as pd
import time
import datetime
from tqdm import tqdm


class Parser():

  def __init__(self, db):
    self.db = db
    self.chrome_options = webdriver.ChromeOptions()
    self.chrome_options.add_argument('--headless')
    self.chrome_options.add_argument('--no-sandbox')
    self.chrome_options.add_argument('--disable-dev-shm-usage')

  def help_infinite_scroll(self):    
    data = open('DS.html','r')
    soup = BeautifulSoup(data, 'html.parser')
    res = []

    for headers, links, time_txt in zip(soup.find_all('div', {'class': 'content-title content-title--short l-island-a'}),
                                        soup.find_all('a', {'class': 'content-link'}),
                                        soup.find_all('time', {'class': 'time'})):
      try:
        header = headers.text.strip()
        link = links.attrs['href']
        time_s = time_txt.attrs['data-date']
        topic = 'Техника'
        res.append([header, link, time_s, topic])
      except Exception as e:
        print(e)
        res.append([np.nan, np.nan, np.nan, np.nan])
        pass

    return res

  def infinite_scroll(self, url, ScrollNumber=10):
    prev=pd.DataFrame()
    SCROLL_PAUSE_TIME = 1
    driver = webdriver.Chrome('chromedriver',options=self.chrome_options)
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")
    df = pd.DataFrame(
        columns={
            'header' : [],
            'link' : [],
            'date' : [],
            'topic' : []
        }
        )

    for i in tqdm(range(1,ScrollNumber)):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
          print('break')  
          break
        last_height = new_height

        file = open('DS.html', 'w')
        file.write(driver.page_source)
        file.close()

        temp = self.help_infinite_scroll()
        df = df.append(pd.DataFrame(temp, columns=['header', 'link', 'date', 'topic']))
        difference = pd.concat([df, prev]).drop_duplicates(keep=False)
        prev = df

        for ind, row in difference.iterrows():
          self.db.insrt(row=row, name_table='news')

    driver.close()

  def help_button_scroll(self):
    data = open('DS.html','r')
    soup = BeautifulSoup(data, 'html.parser')
    res = []

    for headers, links, time_txt, topics in zip(soup.find_all('a', {'class': 'link link_color '}),
                                                soup.find_all('a', {'class': 'cover__link link cover__link_media'}),
                                                soup.find_all('time', {'class': 'date'}),
                                                soup.find_all('a', {'class': 'link link_color'})):
      
      try:
        header = headers.text.strip()
        link = 'https://russian.rt.com' + links.attrs['href']
        time_s = time.mktime(datetime.datetime.strptime(f'{time_txt.attrs["datetime"]}', "%Y-%m-%d %H:%M").timetuple())
        topic = topics.text.strip()
        res.append([header, link, time_s, topic])
      except Exception as e:
        pass
      
    data.close()
    return res

  def button_scroll(self, url, ScrollNumber=10):
    
    prev = pd.DataFrame()
    SCROLL_PAUSE_TIME = 1
    driver = webdriver.Chrome('chromedriver',options=self.chrome_options)
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")
    df = pd.DataFrame(
        columns={
            'header' : [],
            'link' : [],
            'date' : [],
            'topic' : []
        }
        )

    for i in tqdm(range(1,ScrollNumber)):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        continue_link = driver.find_element(By.LINK_TEXT, 'Загрузить ещё')
        driver.execute_script("arguments[0].click();", continue_link)
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print('break')
            break
        last_height = new_height

        file = open('DS.html', 'w')
        file.write(driver.page_source)
        file.close()

        temp = self.help_button_scroll()
        df = df.append(pd.DataFrame(temp, columns=['header', 'link', 'date', 'topic']))
        difference = pd.concat([df, prev]).drop_duplicates(keep=False)
        prev = df

        for ind, row in difference.iterrows():
          self.db.insrt(row=row, name_table='news')

  def process_date(self, date_):
    return time.mktime(datetime.datetime.strptime(f'{date_[:10]}', "%Y-%m-%d").timetuple())

  def parse_walking(self,
                    URL:str = 'https://www.klerk.ru/news/',
                    pages_to_parse:int = 10,
                    page_url = lambda i: f'page/{i}',
                    main_tag:str = 'article',
                    main_class:str = 'feed-item feed-item--normal',
                    tag_of_head:str = 'a',
                    class_of_head:str = 'feed-item__link feed-item-link__check-article',
                    tag_of_date:str = 'core-date-format',
                    #class_of_date:str, #данный параметр не использовался в парсинге klerk.ru, но может быть полезен
                    tag_of_link:str = 'a',
                    class_of_link:str = 'feed-item__link feed-item-link__check-article',
                    cut_page_url = lambda x: x,
                    time_skip = 0.1):
    """
        Парсер.
        Работает с теми новостными страницами, серфинг ленты которых
        осуществляется с помощью переходов на новые url.
        Пример таких источников: klerk.ru
        
        URL: 
             Основная часть ссылки сайта. 
             Пример: https://www.klerk.ru/news/
        page_url: 
             Функция. Аргумент - номер страницы, выход выражения: 
             часть url странички отвечающая за номер страницы. 
             Пример lambda i: f'page/{i}/'
        pages_to_parse: 
             Желаемое количество страниц для парсинга
        main_tag/class: 
             Характеристики html элемента, содержащего 
             осонвную информацию о статье.
        tag_of_head/date/link: 
             HTML тэг для нужного элемента страницы (новость, дата, ссылка)
        class_of_head/date/link: 
             Имя класса для нужного элемента страницы (новость, дата, сыылка)
        process_date_to_int: 
             функция, для форматирования даты. 
             На вход, дата в формате, в котором она указана на сайте, на выход - секунды (int)
             Пример: 
             lambda date_: time.mktime(datetime.datetime.strptime(f'{date_}', "%d.%m.%y").timetuple())
        cut_page_url:
             Функция, которая обрезает url. Используется, когда необходимо 
             корректно склеить две части ссылки.

    """
    process_date_to_int = self.process_date
    df = pd.DataFrame()
    links = []
    heads = []
    dates = []
    for i in tqdm(range(pages_to_parse)):
      page_ref = page_url(i)

      page = requests.get(URL+page_ref)
      soup = bs4.BeautifulSoup(page.content,'html.parser')

      for article in soup.find_all(main_tag,{'class':main_class}):
        ref = article.find(tag_of_link,{'class':class_of_link})['href']
        
        head = article.find(tag_of_head,{'class':class_of_head}).text
        date_ = article.find(tag_of_date)['date']
        seconds = process_date_to_int(date_)
        row = pd.DataFrame([[head, cut_page_url(URL)+ref, seconds, 'Финансы']],
                           columns = ['header', 'link', 'date', 'topic'])
        self.db.insrt(row=row.iloc[0], name_table='news')
      time.sleep(time_skip)

