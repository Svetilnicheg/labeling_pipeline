import pandas as pd
import string
import os
import glob
import locale
import re
import datetime
import functions


# data_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data'
# for_mapping_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/for_mapping'
# data_with_duplicates_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data_with_duplicates/axonQA_{}_with_duplicates.csv'


def from_facts_to_RW_topics(df_m, filedate, cols, data_path, for_mapping_path, data_with_duplicates_path):

    filedate = pd.to_datetime(filedate).strftime('%Y%m%d')

    print(filedate)
    data_with_duplicates_path = data_with_duplicates_path.format(filedate)


    full_logs_path = data_path + '/' + 'axon_qa_{}.csv'.format(filedate)


    df = pd.read_csv(data_with_duplicates_path, sep=';', dtype={'messageId': 'str'}, lineterminator='\n')
    print('С дупликатами ', df.shape)

    print('Без дупликатов ', df_m.shape)
    print('Без дупликатов ', df_m.columns)

    df_m['true_qa'] = 0
    df_m.loc[(df_m.confidence >= .80) & (df_m.result == 'good'), 'true_qa'] = 1
    df_m.loc[df_m.true_qa == 0, 'relevanten_li_otvet'] = 0
    df_m.loc[df_m.true_qa == 1, 'relevanten_li_otvet'] = 1


    print('Сколько говна', df_m[df_m.true_qa == 0].shape)

    functions.clean_data2(df_m, ['query2'])
    functions.clean_data(df, ['query'])


    df['messageId'] = df['messageId'].astype('int')


    x = pd.merge(df, df_m, on=['query2'], how='right')
    x.drop_duplicates('messageId', inplace=True)
    print('без messageId ', x[pd.isna(x.messageId)].shape)
    print('без true_qa ', x[pd.isna(x.true_qa)].shape)
    print('dсего после склейки ', x.shape)



    forTolokaRW = x[(x.true_qa == 1) & (x.axon_debug_intent != 'bing')].copy()

    rw_mId = forTolokaRW.messageId.unique()

    forTolokaRW.rename({'dp_query2': 'INPUT:query', 'dp_pronounceText2': 'INPUT:answer'}, axis=1, inplace=True)

    forTolokaRW = forTolokaRW[['INPUT:query', 'INPUT:answer']].drop_duplicates()
    print('На разметку правильно/не правильно ', forTolokaRW.shape)
    forTolokaRW.to_csv(
        for_mapping_path + '/' + str(filedate) + '/' 'odqa_compressed_{}_for_toloka_rightWrong.tsv'.format(filedate),
        index=False, sep=';')



    x = x[~x.messageId.isin(rw_mId)].copy()
    x.drop_duplicates('messageId', inplace=True)

    for_topic_lbl = x[(x.true_qa == 0)].copy()

    topics_mId = for_topic_lbl.messageId.unique()

    for_topic_lbl.rename({'dp_query2': 'INPUT:query', 'dp_pronounceText2': 'INPUT:answer'}, axis=1, inplace=True)

    for_topic_lbl = for_topic_lbl[['INPUT:query']].drop_duplicates()
    print('На разметку по темам ', for_topic_lbl.shape)
    for_topic_lbl.to_csv(
        for_mapping_path + '/' + str(filedate) + '/' 'odqa_compressed_{}_for_toloka_topics.tsv'.format(filedate),
        index=False, sep=';')

    x = x[~x.messageId.isin(topics_mId)].copy()

    print(x.shape)
    print(x.groupby('axon_debug_intent', as_index=False).true_qa.sum())


    x.rename({'query_x': 'query',
              'pronounceText_x': 'pronounceText'}, axis=1, inplace=True)


    if len(x[pd.isna(x.relevanten_li_otvet)]) != 0:
        print('Есть несматченные данные, необходима ручная проверка.')
        print(x[pd.isna(x.relevanten_li_otvet)])
        raise SystemExit(0)



    x['mapping_source'] = 'facts'

    print('Сколько добавляем', x.shape)


    x[cols].to_csv(full_logs_path, index=False, mode='a', header=False, sep=';')

    print('Данные залиты')#

    return [forTolokaRW, for_topic_lbl]

