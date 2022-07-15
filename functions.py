import pandas
import os
import datetime
import pandas as pd
import tqdm
import glob
import string
import re
import time



def deEmojify(text):
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


def get_prom_data(path, extension, filedate):
    files = []
    data_from_prom = path.format(filedate)
    extension = extension
    # print(files)
    while len(files) == 0:
        os.chdir(data_from_prom)
        files = glob.glob('*.{}'.format(extension))
        if len(files) > 0:
            break
        time.sleep(900)

    df = pd.DataFrame()
    for f in tqdm.tqdm(files):
        df0 = pd.read_csv(data_from_prom + '/' + f, sep=',')
        df0 = df0[~(df0.uid.str.contains('deadbeef') | df0.uid.str.contains('webdbg'))].copy()
        df = df.append(df0)
    print('размер файла за ', filedate, ' ', df.shape)
    return df


def clean_data(dataframe, columns):
    tt = str.maketrans(dict.fromkeys(string.punctuation))
    for col in columns:
        dataframe[col] = dataframe[col].str.lower()
        dataframe[col] = dataframe[col].str.translate(tt)
        dataframe[col].fillna('None', inplace=True)
        dataframe[col] = dataframe[col].apply(lambda x: deEmojify(x))
        dataframe[col] = dataframe[col].str.strip()
        dataframe[col] = dataframe[col].str.replace('<', '')
        dataframe[col] = dataframe[col].str.replace('>', '')


def clean_data2(dataframe, columns):
    tt = str.maketrans(dict.fromkeys(string.punctuation))
    for col in columns:
        col2 = col + '2'
        dataframe[col2] = dataframe[col].str.lower()
        dataframe[col2] = dataframe[col2].str.translate(tt)
        dataframe[col].fillna('None', inplace=True)
        dataframe[col2] = dataframe[col2].apply(lambda x: deEmojify(x))
        dataframe[col2] = dataframe[col2].str.strip()
        dataframe[col2] = dataframe[col2].str.replace('<', '')
        dataframe[col2] = dataframe[col2].str.replace('>', '')
