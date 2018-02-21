import json
import os
from copy import deepcopy
from datetime import date, time

import mysql.connector
import mysql.connector.errors

password = user = 'aniket'
root = 'admin'
database = 'twitter'
host = '127.0.0.1'
directory = os.fsencode('L:/PTDataDownload/trial/')
cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
cursor = cnx.cursor()


def insert_actor(object):
    actor = deepcopy(object['actor'])
    if 'links' in actor:
        del actor['links']
    if 'languages' in actor:
        del actor['languages']
    query = 'INSERT INTO actor ('
    params = 'VALUES ('
    for attribute in actor.keys():
        query = query + attribute + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, actor)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_tweet(object):
    our_object = deepcopy(object)
    del our_object['actor'], our_object['object'], our_object['gnip'], our_object['twitter_entities']
    query = 'INSERT INTO tweet ('
    params = 'VALUES ('
    for attribute in our_object.keys():
        query = query + attribute + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, our_object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_object(object):
    our_object = deepcopy(object['object'])
    rows = ['idobject', 'objectType', 'postedDate', 'postedTime', 'link', 'idtweet', 'summary']
    for k in (our_object.keys() - rows):
        del our_object[k]
    query = 'INSERT INTO object ('
    params = 'VALUES ('
    for attribute in our_object.keys():
        query = query + attribute + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, our_object)
    except mysql.connector.ProgrammingError as err:
        print(err.msg)
    except mysql.connector.DatabaseError as err:
        print(err.msg)


def insert_actor_links(object):
    links = object['actor']['links']
    idactor = object['actor']['idactor']
    query = 'INSERT INTO actor_links (idactor, href, rel)'
    params = 'VALUES (%s, %s, %s)'
    data = []
    for link in links:
        data.append((idactor, link['href'], link['rel']))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_actor_lang(object):
    langs = object['actor']['languages']
    idactor = object['actor']['idactor']
    query = 'INSERT INTO actor_lang (idactor, language)'
    params = 'VALUES (%s, %s)'
    data = []
    for lang in langs:
        data.append((idactor, lang))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_gnip(object):
    rules = object['gnip']['matching_rules']
    idtweet = object['idtweet']
    query = 'INSERT INTO gnip (idtweet, value, tag)'
    params = 'VALUES (%s, %s, %s)'
    data = []
    for rule in rules:
        data.append((idtweet, rule['value'], rule['tag']))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_hashtags(object):
    hashtags = object['twitter_entities']['hashtags']
    idtweet = object['idtweet']
    query = 'INSERT INTO hashtags (idtweet, text, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s)'
    data = []
    for hashtag in hashtags:
        data.append((idtweet, hashtag['text'], hashtag['indices'][0], hashtag['indices'][1]))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_symbols(object):
    symbols = object['twitter_entities']['symbols']
    idtweet = object['idtweet']
    query = 'INSERT INTO symbols (idtweet, text, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s)'
    data = []
    for symbol in symbols:
        data.append((idtweet, symbol['text'], symbol['indices'][0], symbol['indices'][1]))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_urls(object):
    idtweet = object['idtweet']
    urls = object['twitter_entities']['urls']
    query = 'INSERT INTO urls (idtweet, url, expanded_url, display_url, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s)'
    data = []
    for url in urls:
        data.append((idtweet, url['url'], url['expanded_url'], url['display_url'], url['indices'][0],
                     url['indices'][1]))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_mentions(object):
    idtweet = object['idtweet']
    mentions = object['twitter_entities']['user_mentions']
    query = 'INSERT INTO mentions (idtweet, screen_name, name, user_id, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s)'
    data = []
    for mention in mentions:
        data.append((idtweet, mention['screen_name'], mention['name'], mention['id'], mention['indices'][0],
                     mention['indices'][1]))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_media(object):
    idtweet = object['idtweet']
    media = object['twitter_entities']['media']
    query = 'INSERT INTO media (idmedia, idtweet, media_url, media_url_https, url, display_url, expanded_url, type, ' \
            ' start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    data = []
    for medium in media:
        data.append((medium['id'], idtweet, medium['media_url'], medium['media_url_https'], medium['url'],
                     medium['display_url'], medium['expanded_url'], medium['type'], medium['indices'][0],
                     medium['indices'][1]))
    try:
        cursor.executemany(query + params, data)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_media_sizes(object):
    media = object['twitter_entities']['media']
    query = 'INSERT INTO media_sizes (idmedia, size, width, height, resize)'
    params = 'VALUES (%s, %s, %s, %s, %s)'
    data = []
    for medium in media:
        idmedia = medium['id']
        for key in medium['sizes'].keys():
            data.append((idmedia, key, medium['sizes'][key]['w'], medium['sizes'][key]['h'],
                         medium['sizes'][key]['resize']))
        try:
            cursor.executemany(query + params, data)
        except mysql.connector.ProgrammingError as err:
            print(err.msg)
        except mysql.connector.DatabaseError as err:
            print(err.msg)


def format_object(object):
    try:
        if 'postedTime' in object:
            a, b = object['postedTime'].split('T')
            y, m, d = a.split('-')
            object['postedDate'] = date(int(y), int(m), int(d))
            y, m, d = b.split('.')[0].split(':')
            object['postedTime'] = time(int(y), int(m), int(d))
            del a, b, y, m, d
        if 'postedTime' in object['actor']:
            a, b = object['actor']['postedTime'].split('T')
            y, m, d = a.split('-')
            object['actor']['postedDate'] = date(int(y), int(m), int(d))
            y, m, d = b.split('.')[0].split(':')
            object['actor']['postedTime'] = time(int(y), int(m), int(d))
            del a, b, y, m, d
        if 'postedTime' in object['object']:
            a, b = object['object']['postedTime'].split('T')
            y, m, d = a.split('-')
            object['object']['postedDate'] = date(int(y), int(m), int(d))
            y, m, d = b.split('.')[0].split(':')
            object['object']['postedTime'] = time(int(y), int(m), int(d))
            del a, b, y, m, d
        object['object']['idtweet'] = object['idtweet'] = int(object['id'].split(':')[2])
        del object['id']
        if 'in_reply_to' in object:
            object['in_reply_to'] = int(object['in_reply_to']['link'].split('/')[-1])
        object['filter_level'] = object['twitter_filter_level']
        del object['twitter_filter_level']
        object['generator_name'] = object['generator']['displayName']
        object['generator_link'] = object['generator']['link']
        del object['generator']
        object['provider_name'] = object['provider']['displayName']
        object['provider_link'] = object['provider']['link']
        del object['provider']
        object['gnip_lang'] = object['gnip']['language']['value']
        object['idactor'] = object['actor']['idactor'] = int(object['actor']['id'].split(':')[2])
        del object['actor']['id']
        if 'location' in object['actor']:
            object['actor']['location'] = object['actor']['location']['displayName']
        object['actor']['timezone'] = object['actor']['twitterTimeZone']
        del object['actor']['twitterTimeZone']
        if 'verified' in object['actor']:
            if object['actor']['verified']:
                object['actor']['verified'] = 1
            else:
                object['actor']['verified'] = 0
        object['object']['idobject'] = int(object['object']['id'].split(':')[2])
        del object['object']['id']
        if 'body' in object['object']:
            object['object']['summary'] = object['object']['body']
            del object['object']['body']
    except KeyError as ke:
        print(ke)


def unpack_json(filename):
    try:
        with open(filename, encoding='utf-8') as data_file:
            for line in data_file.readlines():
                tweets = line.split('}\r\n{')
                if tweets[0] == '\n':
                    del tweets[0]
                if tweets:
                    json_tweet = json.loads(tweets[0])
                    if 'info' in json_tweet.keys():
                        pass
                    else:
                        format_object(json_tweet)
                        insert_actor(json_tweet)
                        insert_tweet(json_tweet)
                        insert_object(json_tweet)
                        insert_actor_links(json_tweet)
                        insert_actor_lang(json_tweet)

                        insert_gnip(json_tweet)
                        if len(json_tweet['twitter_entities']['hashtags']) > 0:
                            insert_hashtags(json_tweet)
                        if len(json_tweet['twitter_entities']['symbols']) > 0:
                            insert_symbols(json_tweet)
                        if len(json_tweet['twitter_entities']['urls']) > 0:
                            insert_urls(json_tweet)
                        if len(json_tweet['twitter_entities']['user_mentions']) > 0:
                            insert_mentions(json_tweet)
                        if 'media' in json_tweet['twitter_entities']:
                            insert_media(json_tweet)
                            insert_media_sizes(json_tweet)
                        cnx.commit()
    except UnicodeDecodeError as er:
        print(er)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


for file in os.listdir(directory):
    filepath = os.path.join(directory, file)
    unpack_json(filepath)
    print('completed: ', filepath)
