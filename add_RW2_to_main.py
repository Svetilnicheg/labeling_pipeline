import pandas as pd
import locale
import functions

forABC = 1
addData = 1


locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')

pd.set_option('display.max_columns', 500)
desired_width = 320
pd.set_option('display.width', desired_width)
#
# data_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data'
# for_mapping_path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/for_mapping'
# path = '/Users/a19205382/PycharmProjects/QA_statistics/logs_for_kibana/kibana_ETL/data_with_duplicates/axonQA_{}_with_duplicates.csv'.format(
#     filedate)


def add_RW2_to_main(filedate, cols,
                    df_m, data_with_duplicates_path, data_path):
    print(filedate)

    full_logs_path = data_path + '/' + 'axon_qa_{}.csv'.format(filedate)


    df = pd.read_csv(data_with_duplicates_path, sep=';', dtype={'messageId': 'str'}, lineterminator='\n')
    # print(df.columns)
    print('С дупликатами ', df.shape)
    # print(path_mapped_clear_query)
    # print(files)


    print('Без дупликатов ', df_m.shape)
    print('Без дупликатов ', df_m.columns)



    df_m['true_qa'] = 1
    df_m['relevanten_li_otvet'] = 0
    df_m.loc[(df_m.result == 'good'), 'relevanten_li_otvet'] = 1



    print('Сколько говна', df_m[df_m.relevanten_li_otvet == 0].shape)

    functions.clean_data(df_m, ['query2', 'dp_pronounceText2'])
    functions.clean_data(df, ['query', 'dp_pronounceText'])
    # df_m['query2'] = df_m['query2'].str.lower()
    # df_m['query2'] = df_m['query2'].str.translate(tt)
    # df_m['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df_m['query2'] = df_m['query2'].apply(lambda x: deEmojify(x))
    # df_m['query2'] = df_m['query2'].str.strip()



    # df['query2'] = df['query'].str.lower()
    # df['query2'] = df['query2'].str.translate(tt)
    # df['query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df['query2'] = df['query2'].apply(lambda x: deEmojify(x))
    # df['query2'] = df['query2'].str.strip()

    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.lower()
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.translate(tt)
    # df['dp_pronounceText2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].apply(lambda x: deEmojify(x))
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.strip()

    # df_m['dp_pronounceText2'] = df_m['dp_pronounceText2'].str.lower()
    # df_m['dp_pronounceText2'] = df_m['dp_pronounceText2'].str.translate(tt)
    # df_m['dp_pronounceText2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    # df_m['dp_pronounceText2'] = df_m['dp_pronounceText2'].apply(lambda x: deEmojify(x))
    # df_m['dp_pronounceText2'] = df_m['dp_pronounceText2'].str.strip()



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



    x['mapping_source'] = 'rightWrong2'

    print('Сколько добавляем', x.shape)
    x[cols].to_csv(full_logs_path, index=False, mode='a', header=False, sep=';')




