import pandas as pd
import string
import os
import glob
import locale
import re
import datetime
import functions
forABC = 1
addData = 1


def deEmojify(text):
    regrex_pattern = re.compile(pattern="["
                                        u"\U0001F600-\U0001F64F"  # emoticons
                                        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                        u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                        "]+", flags=re.UNICODE)
    return regrex_pattern.sub(r'', text)


locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

pd.set_option('display.max_columns', 500)
desired_width = 320
pd.set_option('display.width', desired_width)

# data_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data'
# for_mapping_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/for_mapping'

tt = str.maketrans(dict.fromkeys(string.punctuation))

def RW_add_to_main(cols,filedate, df_m, data_with_duplicates_path, data_path):
    filedate = pd.to_datetime(filedate).strftime('%Y%m%d')
    print(filedate)
    data_with_duplicates_path = data_with_duplicates_path.format(filedate)

    full_logs_path = data_path + '/' + 'axon_qa_{}.csv'.format(filedate)

    df = pd.read_csv(data_with_duplicates_path, sep=';', dtype={'messageId': 'str'}, lineterminator='\n')
    print('С дупликатами ', df.shape)


    df_m['true_qa'] = 1
    df_m['relevanten_li_otvet'] = 0
    df_m.loc[(df_m.result == 'good'), 'relevanten_li_otvet'] = 1




    print('Сколько говна', df_m[df_m.relevanten_li_otvet == 0].shape)
    print('Сколько без relevanten_li_otvet ', df_m[pd.isna(df_m.relevanten_li_otvet)].shape)

    functions.clean_data(df_m, ['query2', 'dp_pronounceText2'])

    # df_m['query2'] = df_m['query2'].str.lower()
    # df_m['query2'] = df_m['query2'].str.translate(tt)
    # df_m['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df_m['query2'] = df_m['query2'].apply(lambda x: deEmojify(x))
    # df_m['query2'] = df_m['query2'].str.strip()

    functions.clean_data2(df, ['query', 'dp_pronounceText'])

    # df['query2'] = df['query'].str.lower()
    # df['query2'] = df['query2'].str.translate(tt)
    # df['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df['query2'] = df['query2'].apply(lambda x: deEmojify(x))
    # df['query2'] = df['query2'].str.strip()



    df['messageId'] = df['messageId'].astype('int')


    x = pd.merge(df, df_m, on=['query2', 'dp_pronounceText2'], how='right')
    x.drop_duplicates('messageId', inplace=True)
    print('без messageId ', x[pd.isna(x.messageId)].shape)


    x.drop_duplicates('messageId', inplace=True)

    #     x = x[~pd.isna(x.relevanten_li_otvet)].copy()
    if len(x[pd.isna(x.confidence)]) != 0:
        print('Есть несматченные данные, необходима ручная проверка.')
        print(x[pd.isna(x.relevanten_li_otvet)])
        raise SystemExit(0)





    x['mapping_source'] = 'rightWrong1'

    print('Сколько добавляем', x.shape)

    x[cols].to_csv(full_logs_path, index=False, mode='a', header=False, sep=';')
