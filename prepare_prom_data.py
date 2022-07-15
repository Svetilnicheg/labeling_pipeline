import pandas as pd
import string
import os
import json
import functions


def prepare_prom_data(df, cache, promos, filedatedt, data_path, data_with_duplicates_path, for_mapping_path, cols):

    filedate = pd.to_datetime(filedatedt).strftime('%Y%m%d')


    print('дубликаты')
    print(df.shape)
    df.drop_duplicates('message_id', inplace=True)
    print(df.shape)
    print(df.groupby('dp_query', as_index=False).message_id.count().sort_values('message_id', ascending=False).head(20))


    df['date2'] = df['date'].apply(lambda x: x.split('.')[0].split('@')[0].strip())
    df['time'] = df['date'].apply(lambda x: x.split('.')[0].split('@')[1].strip())
    df['date2'] = pd.to_datetime(df['date2'], infer_datetime_format=True)
    df['timestamp'] = pd.to_datetime(df['date2'].astype('str') + ' ' + df['time'])

    print(df.shape)
    print(df['date2'].unique())

    df = df[df['date2'] == pd.to_datetime(filedatedt)].copy()
    print(df.shape)

    df['ir_debug_info'] = df.debug_info.apply(
        lambda x: json.loads(x)['intent_recognizer'][0] if 'intent_recognizer' in json.loads(x).keys() else None)

    df['axon_debug_info'] = df.debug_info.apply(
        lambda x: json.loads(x)['axon'][0] if 'axon' in json.loads(x).keys() else None)

    df['source_for_run_app_deeplink'] = df['axon_debug_info'].apply(lambda x: x.get('source_for_run_app_deeplink', None))

    df['hint'] = df.ir_debug_info.apply(lambda x: ((x['node_predictions']['hinted'].get('axon_qa', 0)
                                                    if 'hinted' in x['node_predictions'] else 0)
                                                   if 'node_predictions' in x.keys() else 0) if x is not None else 0)


    print('source_for_run_app_deeplink', ': ', df['source_for_run_app_deeplink'].unique())


    df['dp_pronounceText'] = df['axon_debug_info'].apply(lambda x: x['qa_system'].get('qa_answer', None))

    df.pronounce_text.fillna(df.show_text, inplace=True)
    df.pronounce_text.fillna(df['dp_pronounceText'], inplace=True)
    df.pronounce_text.fillna('None', inplace=True)

    df.dp_pronounceText.fillna(df.show_text, inplace=True)

    df['dp_pronounceText'] = df.dp_pronounceText.str.replace(';', '')


    df['widget'] = df['axon_debug_info'].apply(lambda x: x['qa_system'].get('is_widget', None))
    df['answer_type'] = df['axon_debug_info'].apply(lambda x: x['qa_system']['odqa'].get('non_text_answer_type', None)
        if 'odqa' in x['qa_system'].keys() else None)

    df['answer_source'] = df.axon_debug_info.apply(lambda x: x['qa_system']['odqa'].get('answer_source', None)
        if 'odqa' in x['qa_system'].keys() else None)

    df.loc[df.pronounce_text.str.contains('<audio text'), 'answer_type'] = 'sound'
    df.loc[(df.pronounce_text.str.contains('<audio text')) & (pd.isna(df.dp_pronounceText)), 'dp_pronounceText'] = 'sound'


    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    df = df[df['date'] == pd.to_datetime(filedatedt)].copy()


    df.reset_index(inplace=True)

    print("Пустой dp_query")
    print(df.loc[pd.isna(df.dp_query), ['dp_query', 'pronounce_text', 'qa_intent']])

    df.dp_query.fillna('Пришла пустая строка', inplace=True)
    df.loc[df.qa_intent == 'rasskazhi_stihi_majakovskogo', 'dp_query2'] = 'расскажи стихи маяковского'

    df['dp_pronounceText'].fillna(df.show_text, inplace=True)
    df['dp_pronounceText'].fillna('None', inplace=True)

    functions.clean_data2(df, ['dp_query', 'dp_pronounceText'])
    # df['dp_query2'] = df['dp_query'].str.lower()
    # df['dp_query2'] = df['dp_query2'].str.replace(',', '')
    # df['dp_query2'] = df['dp_query2'].str.translate(tt)
    # df['dp_query2'] = df['dp_query2'].apply(lambda x: functions.deEmojify(x))
    # df['dp_query2'] = df['dp_query2'].str.strip()
    # df['dp_query2'].fillna('в_вопросе_было_только_числительное', inplace=True)
    #
    # df['dp_pronounceText2'] = df['dp_pronounceText'].str.lower()
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.translate(tt)
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].apply(lambda x: functions.deEmojify(x))
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.strip()
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.replace('<', '')
    # df['dp_pronounceText2'] = df['dp_pronounceText2'].str.replace('>', '')


    df['ytochneniya'] = 0
    df['ytochneniya'] = df.axon_debug_info.apply(lambda x: x['qa_system'].get('voiced_suggest', 0))

    df.loc[df.ytochneniya.isin([True, 'True', '1.0']), 'ytochneniya'] = 1
    df.loc[df.ytochneniya.isin([False, 'False', '0.0', None]), 'ytochneniya'] = 0

    if 'surface'not in df.columns:
        df['surface'] = df.axon_debug_info.apply(lambda x: x['qa_system'].get('surface', None))



    df.loc[df['dp_query'] == 'МатрицаПромо', 'source_for_run_app_deeplink'] = 'pushMatrix'

    for i in range(0, len(promos)):
        df.loc[
            (df['dp_query2'].str.strip() == promos['text'][i])
            & (df['timestamp'].between(promos['dt_start'][i], promos['dt_end'][i]))
            & (pd.isna(df.widget))
            & (promos.surface[i] == df.surface)
            , 'promo_type'] = promos.loc[i, 'type']

        df.loc[
            (df['dp_query2'].str.strip() == promos['text'][i])
            & (df['timestamp'].between(promos['dt_start'][i], promos['dt_end'][i]))
            & (pd.isna(df.widget))
            & (promos.surface[i] == df.surface)
            , 'promo_campain'] = promos.loc[i, 'campain']



    df['gallery'] = 0
    df['gallery'] = df.axon_debug_info.apply(lambda x:
                                             x['qa_system']['odqa'].get('answer_with_gallery', 0)
                                             if 'odqa' in x['qa_system'].keys() else 0)

    df.loc[df.gallery.isin([True, 'True', '1.0']), 'gallery'] = 1
    df.loc[df.gallery.isin([False, 'False', '0.0', None]), 'gallery'] = 0

    df['is_from_gallery_click'] = df.axon_debug_info.apply(lambda x:
                                                          x['qa_system'].get('is_from_gallery_click', None))
    df['voice_or_keyboard_input'] = df.axon_debug_info.apply(lambda x:
                                                          x['qa_system'].get('voice_or_keyboard_input', None))


    df['game_type'] = df.axon_debug_info.apply(lambda x:
                                                 x['qa_system']['game'].get('game_type', None)
                                                 if 'game' in x['qa_system'].keys()
                                                 else None)


    df['qa_intent'] = df.axon_debug_info.apply(lambda x: x['qa_system'].get('qa_intent', None))
    print('Empty qa_intent')


    df.loc[pd.isna(df.answer_type), 'answer_type'] = 'Карточка без картинки'
    df.loc[(df.gallery == 1), 'answer_type'] = 'Галерея'
    df.loc[(df.answer_type == 'sound'), 'answer_type'] = 'Звуковой ответ'
    df.loc[(df.answer_type == 'pic'), 'answer_type'] = 'Ответ с картинкой'
    df.loc[(df.answer_type == 'pic') & (pd.isna(df.dp_pronounceText)), 'answer_type'] = 'Ответ картинкой'
    df.loc[(df.qa_intent == 'bing'), 'answer_type'] = 'bing'
    df.loc[~pd.isna(df.game_type), 'answer_type'] = 'Игры'


    df['source'] = 'Чистый QA'
    df.loc[~pd.isna(df.promo_type), 'source'] = df.loc[~pd.isna(df.promo_type), 'promo_type']
    df.loc[df.hint == 1, 'source'] = 'Cаджесты'
    df.loc[df.ytochneniya == 1, 'source'] = 'Озвученные саджесты'
    df.loc[(~pd.isna(df.widget)) & (df.widget != 'greeting'), 'source'] = 'Виджет'
    df.loc[(df.widget == 'greeting'), 'source'] = 'Гритинг'
    df.loc[df.is_from_gallery_click == True, 'source'] = 'Клик c галереи'
    df.loc[~pd.isna(df.source_for_run_app_deeplink), 'source'] = df.loc[~pd.isna(df.source_for_run_app_deeplink), 'source_for_run_app_deeplink']
    df.loc[~pd.isna(df.game_type), 'source'] = 'Игры'

    df['source2'] = 'Чистый QA'
    df.loc[~pd.isna(df.promo_type), 'source2'] = df.loc[~pd.isna(df.promo_type), 'promo_campain']
    df.loc[df.hint == 1, 'source2'] = 'Cаджесты'
    df.loc[df.ytochneniya == 1, 'source2'] = 'Озвученные саджесты'
    df.loc[~pd.isna(df.widget), 'source2'] = df.loc[~pd.isna(df.widget), 'widget']
    df.loc[df.is_from_gallery_click == True, 'source2'] = 'Клик c галереи'
    df.loc[~pd.isna(df.source_for_run_app_deeplink), 'source2'] = df.loc[~pd.isna(df.source_for_run_app_deeplink), 'source_for_run_app_deeplink']
    df.loc[~pd.isna(df.game_type), 'source2'] = df.loc[~pd.isna(df.game_type), 'game_type']


    df.loc[df.answer_type == 'Галерея', 'dp_pronounceText'] = df.loc[df.answer_type == 'Галерея', 'dp_pronounceText'] \
                                                              + ' [Пояснение: После текста идёт подбока фактов по теме.]'


    df.rename({'message_id': 'messageId',
              'channel': 'device',
              'uid': 'userId',
              'qa_intent': 'axon_debug_intent',
              'dp_query': 'query',
              'dp_pronounceText': 'pronounceText'}, axis=1, inplace=True)

    df['pronounceText'].fillna('Нет ответа', inplace=True)

    df['is_wiki_fallback'] = df.axon_debug_info.apply(lambda x: x['qa_system']['odqa'].get('is_wiki_fallback', None)
    if 'odqa' in x['qa_system'].keys() else None)
    df.loc[~pd.isna(df['is_wiki_fallback']), 'axon_debug_intent'] = 'wiki_fallback'

    print('разбивка по интентам')
    print(df.groupby('axon_debug_intent', as_index=False).messageId.count())


    x = df[(
             ~(pd.isna(df.promo_campain))
            | (df.answer_type.isin(['Звуковой ответ', 'Ответ картинкой']))
            | (df.is_from_gallery_click == True)
            | (df.ytochneniya == 1)
            | ~(pd.isna(df.widget))
            | (df['axon_debug_intent'] == 'promise_of_delayed_answer')
            | ~(pd.isna(df.game_type))
             | (~pd.isna(df.source_for_run_app_deeplink))
             | (df['query'] == "Сколько я могу сэкономить?")
            )].copy()



    x['relevanten_li_otvet'] = 1
    x['true_qa'] = 1
    x.loc[x.axon_debug_intent == 'ne_znayu', 'relevanten_li_otvet'] = 0
    x.loc[~pd.isna(x.promo_type), 'relevanten_li_otvet'] = 1

    x.loc[x['axon_debug_intent'].isin(['ne_znayu']), 'relevanten_li_otvet'] = 0



    y = df[~(
             ~(pd.isna(df.promo_campain))
             | (df.answer_type == 'Звуковой ответ')
             | (df.is_from_gallery_click == True)
             | (df.ytochneniya == 1)
             | ~(pd.isna(df.widget))
             | (df['axon_debug_intent'] == 'promise_of_delayed_answer')
             | ~(pd.isna(df.game_type))
             | (~pd.isna(df.source_for_run_app_deeplink))
             | (df['query'] == "Сколько я могу сэкономить?")
             )].copy()



    y0 = y.loc[:, ~y.columns.isin(['relevanten_li_otvet', 'true_qa'])].copy()



    yy = pd.merge(y0, cache, on=['dp_query2', 'dp_pronounceText2'], how='left')

    messageId_cashed = yy[~(pd.isna(yy.relevanten_li_otvet))].messageId.unique()

    print('Добавим кэш в уже известные данные')
    ################# Добавим кэш в уже известные данные
    yy = yy[~(pd.isna(yy.relevanten_li_otvet))].copy()

    yy.drop_duplicates('messageId', inplace=True)



    yy['mapping_source'] = 'cache'


    x['mapping_source'] = 'GWEqS'
    x = x.append(yy)

    print('опасный момент')
    print(x.shape)
    x.drop_duplicates(['messageId'], inplace=True)
    print(x.shape)

    y = y[~y.messageId.isin(messageId_cashed)].copy()



    print('Создаю файлы на разметку')


    if not os.path.exists(for_mapping_path + '/' + filedate):
        os.makedirs(for_mapping_path + '/' + filedate)


    y.to_csv(data_with_duplicates_path + '/' + 'axonQA_{}_with_duplicates.csv'.format(filedate), index=False, sep=';')


    y.rename({'query': 'INPUT:query'}, axis=1, inplace=True)

    y = y['INPUT:query'].drop_duplicates()

    print('На разметку ', y.shape)

    y.to_csv(
        for_mapping_path + '/' + filedate + '/' 'axonQA_compressed_{}_clearQuery_for_toloka.tsv'.format(filedate),
        index=False, sep='\t')



    print('Создаю файл с кэшем и доверенными запросами')


    print(x[cols].shape)


    print(x.groupby(['source', 'axon_debug_intent'], as_index=False).messageId.count())

    print(x.groupby(['source', 'answer_type'], as_index=False).messageId.count())


    x[cols].to_csv(data_path + '/' + 'axon_qa_{}.csv'.format(filedate), index=False, sep=';')
    print('Файл axon_qa_{}.csv создан. Данные записаны'.format(filedate))

    return y



