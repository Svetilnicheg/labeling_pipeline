import pandas as pd
import string
import os
import glob
import locale
import re
import datetime
import functions

locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

pd.set_option('display.max_columns', 500)
desired_width = 320
pd.set_option('display.width', desired_width)
#
# data_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data'
# for_mapping_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/for_mapping'
# path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data_with_duplicates/axonQA_{}_with_duplicates.csv'.format(
#     filedate)
#
# path_mapped_clear_query = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/toloka_mapped/{}/topics'.format(
#     filedate)


def topics_to_main_toRW2(filedate, cols, df_m, data_path, data_with_duplicates_path, for_mapping_path):
    filedate = pd.to_datetime(filedate).strftime('%Y%m%d')
    print(filedate)

    full_logs_path = data_path + '/' + 'axon_qa_{}.csv'.format(filedate)


    df = pd.read_csv(data_with_duplicates_path, sep=';', dtype={'messageId': 'str'}, lineterminator='\n')
    # print(path)
    print('С дупликатами ', df.shape)

    print('Без дупликатов ', df_m.shape)
    # print('Без дупликатов ', df_m.columns)

    print(df_m.result.unique())


    df_m.loc[df_m.result.isin(['game', 'video', 'music', 'entertainment', 'other', 'idk'],), 'true_qa'] = 0
    df_m.loc[df_m.result.isin(['game', 'video', 'music', 'entertainment', 'other', 'idk']), 'relevanten_li_otvet'] = 0

    df_m.loc[(df_m.result == 'facts') & (df_m.confidence <= 0.8), 'true_qa'] = 0
    df_m.loc[(df_m.result == 'facts') & (df_m.confidence <= 0.8), 'relevanten_li_otvet'] = 0


    print('Сколько говна', df_m[~pd.isna(df_m.relevanten_li_otvet)].shape)

    functions.clean_data(df_m, ['query2'])


    # df_m['query2'] = df_m['query2'].str.lower()
    # df_m['query2'] = df_m['query2'].str.translate(tt)
    # df_m['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df_m['query2'] = df_m['query2'].apply(lambda x: deEmojify(x))
    # df_m['query2'] = df_m['query2'].str.strip()
    #
    #
    functions.clean_data2(df_m, ['query'])
    # df['query2'] = df['query'].str.lower()
    # df['query2'] = df['query2'].str.translate(tt)
    # df['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df['query2'] = df['query2'].apply(lambda x: deEmojify(x))
    # df['query2'] = df['query2'].str.strip()
    #
    #
    # #
    df['messageId'] = df['messageId'].astype('int')
    #
    x = pd.merge(df, df_m, on=['query2'], how='right')
    x.drop_duplicates('messageId', inplace=True)
    print('без messageId ', x[pd.isna(x.messageId)].shape)
    x.drop_duplicates('messageId', inplace=True)


    x.loc[(x.axon_debug_intent == 'bing') & (pd.isna(x.true_qa)), 'relevanten_li_otvet'] = 1
    x.loc[(x.axon_debug_intent == 'bing') & (pd.isna(x.true_qa)), 'true_qa'] = 1



    x.loc[x.axon_debug_intent == 'ne_znayu', 'relevanten_li_otvet'] = 0
    x.loc[(x.axon_debug_intent == 'ne_znayu') & (pd.isna(x.true_qa)), 'true_qa'] = 1

    x.rename({'query_x': 'query',
              'pronounceText_x': 'pronounceText'}, axis=1, inplace=True)

    # #     x = x[~pd.isna(x.relevanten_li_otvet)].copy()
    if len(x[pd.isna(x.confidence)]) != 0:
        print('Есть несматченные данные, необходима ручная проверка.')
        print(x[pd.isna(x.relevanten_li_otvet)])
        raise SystemExit(0)


    rw2 = x[pd.isna(x.relevanten_li_otvet)][['query', 'pronounceText']].copy()
    rw2.rename({'query': 'INPUT:query', 'pronounceText': 'INPUT:answer'}, axis=1, inplace=True)
    rw2.to_csv(for_mapping_path+'/{}/rightWrong2_to_toloka_{}.csv'.format(filedate, filedate), sep=';', index=False)

    print('на доразметку ', rw2.shape)

    x = x[~pd.isna(x.relevanten_li_otvet)].copy()

    print('без true_qa ', x[pd.isna(x.true_qa)].shape)
    print('без relevanten_li_otvet ', x[pd.isna(x.relevanten_li_otvet)].shape)



    x['mapping_source'] = 'topics'
    print('Добавляем ', x[cols].shape)
    #

    x[cols].to_csv(full_logs_path, index=False, mode='a', header=False, sep=';')

    return rw2
