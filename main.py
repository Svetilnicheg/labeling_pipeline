import pandas
import os
import datetime
import pandas as pd
import sys
import tqdm
# import Toloka_functions
import time
import glob
import string
import re
import locale
import functions
import json
import toloka.client as toloka
import Toloka_functions
from prepare_prom_data import prepare_prom_data
from from_facts_to_RW_topics import from_facts_to_RW_topics
from multiprocessing import Process, Queue
from RW_add_to_main import RW_add_to_main
from topics_to_main_to_RW2 import topics_to_main_toRW2
from add_RW2_to_main import add_RW2_to_main


def check_pool_status_and_get_data(pool_id, toloka_client, project, filedate, config):
    while True:
        if (str(toloka_client.get_pool(pool_id=pool_id).status) == 'Status.CLOSED') \
                & (str(toloka_client.get_pool(pool_id=pool_id).last_close_reason) == 'CloseReason.COMPLETED'):
            print(time.time())
            print('Пул {} размечен'.format(pool_id))
            return Toloka_functions.get_from_toloka(toloka_client, project, filedate, config, pool_id)
            break

        else:
            print(time.time())
            print('Пул {} в работе'.format(pool_id))
        time.sleep(900)

locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

pd.set_option('display.max_columns', 500)
desired_width = 320
pd.set_option('display.width', desired_width)

cols = ["timestamp", "userId", "messageId", "query", "pronounceText", "true_qa", "relevanten_li_otvet",
        "device", "axon_debug_intent", "axon_debug_info", "source", "mapping_source", "answer_source", "scenario",
        "current_app", "sub", "device_id", "character", "uuid", "ir_debug_info", "answer_type", "source2", "is_male",
        "birthyear", "city", "country", "gender", "location", "region", "subregion"]

with open('config.json') as f:
    config = json.load(f)



prom_data_path = config['paths']['prom_data_path']
promo_path = config['paths']['promo_path']
cache_path = config['paths']['cache_path']
data_path = config['paths']['data_path']
data_with_duplicates_path = config['paths']['data_with_duplicates_path']
for_mapping_path = config['paths']['for_mapping_path']


# print(data_with_duplicates_path)
TOKEN = config['toloka']['TOKEN']


filedate = (pd.to_datetime(datetime.datetime.now()) - datetime.timedelta(days=1)).strftime('%Y%m%d')
print(filedate)
df = functions.get_prom_data(prom_data_path, 'csv', filedate)


promos = pd.read_csv(promo_path, sep=';')

promos['dt_start'] = pd.to_datetime(promos['dt_start'])
promos['dt_end'] = pd.to_datetime(promos['dt_end'])

filedatedt = pd.to_datetime(filedate).date().strftime('%Y-%m-%d')

promos = promos[(promos.dt_end >= filedatedt) & (promos.dt_start <= filedatedt)].copy()
promos.reset_index(inplace=True)
functions.clean_data(promos, ['text'])


cache = pd.read_csv(cache_path, sep=',')
functions.clean_data(cache, ['dp_pronounceText2', 'dp_query2'])

to_toloka_facts = pd.DataFrame(prepare_prom_data(df, cache, promos, filedatedt,
                                              data_path, data_with_duplicates_path, for_mapping_path, cols))

del cache
del promos

to_toloka_facts.rename({'INPUT:query': 'query'}, axis=1, inplace=True)

toloka_client = toloka.TolokaClient(TOKEN, 'PRODUCTION')

print(to_toloka_facts)
pool_id = Toloka_functions.send_to_toloka(toloka_client, 'facts', filedate, to_toloka_facts, config)

del to_toloka_facts

while True:
    if (str(toloka_client.get_pool(pool_id=pool_id).status) == 'Status.CLOSED') \
            & (str(toloka_client.get_pool(pool_id=pool_id).last_close_reason) == 'CloseReason.COMPLETED'):
        print(time.time())
        print('Пул {} размечен'.format(pool_id))
        break

    else:
        print(time.time())
        print('Пул {} в работе'.format(pool_id))
    time.sleep(900)

from_toloka_facts = Toloka_functions.get_from_toloka(toloka_client, 'facts', filedate, config, pool_id)


forTolokaRW, for_topic_lbl = from_facts_to_RW_topics(from_toloka_facts, filedate, cols,
                                                     data_path, for_mapping_path, data_with_duplicates_path)


# pool_id_rw = Toloka_functions.send_to_toloka(toloka_client, 'rightWrong', filedate, to_toloka_facts, config)

queue1 = Queue()
queue2 = Queue()  # create a queue object
p1 = Process(target=Toloka_functions.send_to_toloka, args=(toloka_client, 'rightWrong', filedate,
                                                           to_toloka_facts, config, queue1))  # we're setting 3rd argument to queue1
p1.start()
p2 = Process(target=Toloka_functions.send_to_toloka, args=(toloka_client, 'topics', filedate,
                                                           to_toloka_facts, config, queue2))
p2.start()
pool_id_rw = queue1.get()
pool_id_topic = queue2.get()
p1.join()
p2.join()




queue1 = Queue()
queue2 = Queue()
p1 = Process(target=check_pool_status_and_get_data, args=(pool_id_rw, toloka_client, 'rightWrong',
                                                          filedate, config, queue1))
p1.start()
p2 = Process(target=check_pool_status_and_get_data, args=(pool_id_topic, toloka_client, 'topics',
                                                          filedate, config, queue2))

p2.start()
from_toloka_rw = queue1.get()
from_toloka_topics = queue2.get()
p1.join()
p2.join()

RW_add_to_main(cols, filedate, from_toloka_rw, data_with_duplicates_path, data_path)

rw2 = topics_to_main_toRW2(filedate, cols, from_toloka_topics, data_path, data_with_duplicates_path, for_mapping_path)

pool_id_rw2 = Toloka_functions.send_to_toloka(toloka_client, 'rightWrong2', filedate, to_toloka_facts, config)

rw2 = check_pool_status_and_get_data(pool_id_rw2, toloka_client, 'rightWrong2', filedate, config)

add_RW2_to_main(filedate, cols,
                    rw2, data_with_duplicates_path, data_path)
