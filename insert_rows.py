import json
import os
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
    actor = object['actor']
    rows = {'id': 'idactor', 'objectType': 'objectType', 'link': 'link', 'displayName': 'displayName',
            'postedDate': 'postedDate', 'postedTime': 'postedTime', 'image': 'image', 'summary': 'summary',
            'friendsCount': 'friendsCount', 'followersCount': 'followersCount', 'listedCount': 'listedCount',
            'statusesCount': 'statusesCount', 'favoritesCount': 'favoritesCount', 'twitterTimeZone': 'timezone',
            'verified': 'verified', 'utcOffset': 'utcOffset', 'preferredUsername': 'preferredUsername',
            'location': 'location'}
    attributes = sorted(list(rows.keys() & actor.keys()))
    actor['id'] = int(actor['id'].split(':')[2])
    if 'links' in actor:
        del actor['links']
    if 'languages' in actor:
        del actor['languages']
    if 'postedTime' in attributes:
        a, b = actor['postedTime'].split('T')
        y, m, d = a.split('-')
        h, mi, s = b.split('.')[0].split(':')
        actor['postedDate'] = date(int(y), int(m), int(d))
        actor['postedTime'] = time(int(h), int(mi), int(s))
        attributes.insert(attributes.index('postedTime'), 'postedDate')
        del a, b, y, m, d, h, mi, s
    if 'location' in attributes:
        actor['location'] = actor['location']['displayName']
    if 'verified' in attributes:
        if actor['verified']:
            actor['verified'] = 1
        else:
            actor['verified'] = 0
    query = 'INSERT INTO actor ('
    params = 'VALUES ('
    for attribute in attributes:
        query = query + rows[attribute] + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, actor)
        cnx.commit()
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_tweet(object):
    object['id'] = int(object['id'].split(':')[2])
    object['idactor'] = int(object['actor']['id'].split(':')[2])
    object['generator_name'] = object['generator']['displayName']
    object['generator_link'] = object['generator']['link']
    object['provider_name'] = object['provider']['displayName']
    object['provider_link'] = object['provider']['link']
    object['gnip_lang'] = object['gnip']['language']['value']
    rows = {'id': 'idtweet', 'objectType': 'objectType', 'verb': 'verb', 'postedDate': 'postedDate',
            'postedTime': 'postedTime', 'link': 'link', 'body': 'body', 'favoritesCount': 'favoritesCount',
            'twitter_filter_level': 'filter_level', 'twitter_lang': 'twitter_lang', 'retweetCount': 'retweetCount',
            'idactor': 'idactor', 'generator_name': 'generator_name', 'generator_link': 'generator_link',
            'provider_name': 'provider_name', 'provider_link': 'provider_link', 'gnip_lang': 'gnip_lang'}
    attributes = sorted(list(rows.keys() & object.keys()))
    if 'postedTime' in attributes:
        a, b = object['postedTime'].split('T')
        y, m, d = a.split('-')
        h, mi, s = b.split('.')[0].split(':')
        object['postedDate'] = date(int(y), int(m), int(d))
        object['postedTime'] = time(int(h), int(mi), int(s))
        attributes.insert(attributes.index('postedTime'), 'postedDate')
        del a, b, y, m, d, h, mi, s
    del object['actor'], object['generator'], object['provider'], object['object'], object['twitter_entities']
    del object['gnip']
    query = 'INSERT INTO tweet ('
    params = 'VALUES ('
    for attribute in attributes:
        query = query + rows[attribute] + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_object(object):
    object['object']['idtweet'] = int(object['id'].split(':')[2])
    object = object['object']
    object['id'] = int(object['id'].split(':')[2])
    rows = {'id': 'idobject', 'objectType': 'objectType', 'postedDate': 'postedDate', 'postedTime': 'postedTime',
            'link': 'link', 'body': 'summary', 'idtweet': 'idtweet', 'summary': 'summary'}
    attributes = sorted(list(rows.keys() & object.keys()))
    if 'postedTime' in attributes:
        a, b = object['postedTime'].split('T')
        y, m, d = a.split('-')
        h, mi, s = b.split('.')[0].split(':')
        object['postedDate'] = date(int(y), int(m), int(d))
        object['postedTime'] = time(int(h), int(mi), int(s))
        attributes.insert(attributes.index('postedTime'), 'postedDate')
        del a, b, y, m, d, h, mi, s
    for k in (object.keys() - attributes):
        del object[k]
    query = 'INSERT INTO object ('
    params = 'VALUES ('
    for attribute in attributes:
        query = query + rows[attribute] + ', '
        params = params + '%(' + attribute + ')s, '
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    try:
        cursor.execute(query + params, object)
    except mysql.connector.ProgrammingError as err:
        print(err.msg)
    except mysql.connector.DatabaseError as err:
        print(err.msg)


def insert_actor_links(object):
    idactor = int(object['actor']['id'].split(':')[2])
    links = object['actor']['links']
    del object
    query = 'INSERT INTO actor_links ('
    params = 'VALUES ('
    link = links.pop()
    link['idactor'] = idactor
    rows = {'idactor': 'idactor', 'href': 'href', 'rel': 'rel'}
    attributes = sorted(list(rows.keys() & link.keys()))
    object = []
    temp = []
    for attribute in attributes:
        temp.append(link[attribute])
        query = query + rows[attribute] + ', '
        params = params + '%s, '
    object.append(tuple(temp))
    query = query.strip(', ')
    query = query + ') '
    params = params.strip(', ')
    params = params + ')'
    while len(links) > 0:
        link = links.pop()
        link['idactor'] = idactor
        attributes = sorted(list(rows.keys() & link.keys()))
        temp = []
        for attribute in attributes:
            temp.append(link[attribute])
        object.append(tuple(temp))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_actor_lang(object):
    idactor = int(object['actor']['id'].split(':')[2])
    langs = object['actor']['languages']
    del object
    query = 'INSERT INTO actor_lang (idactor, language)'
    params = 'VALUES (%s, %s)'
    object = []
    for l in langs:
        object.append((idactor, l))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_gnip(object):
    idtweet = int(object['id'].split(':')[2])
    rules = object['gnip']['matching_rules']
    del object
    query = 'INSERT INTO gnip (idtweet, value, tag)'
    params = 'VALUES (%s, %s, %s)'
    object = []
    for rule in rules:
        object.append((idtweet, rule['value'], rule['tag']))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_hashtags(object):
    idtweet = int(object['id'].split(':')[2])
    del object
    hashtags = object['twitter_entities']['hashtags']
    query = 'INSERT INTO hashtags (idtweet, text, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s)'
    object = []
    for hashtag in hashtags:
        object.append((idtweet, hashtag['text'], hashtag['indices'][0], hashtag['indices'][1]))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_symbols(object):
    idtweet = int(object['id'].split(':')[2])
    del object
    symbols = object['twitter_entities']['symbols']
    query = 'INSERT INTO symbols (idtweet, text, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s)'
    object = []
    for symbol in symbols:
        object.append((idtweet, symbol['text'], symbol['indices'][0], symbol['indices'][1]))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_urls(object):
    idtweet = int(object['id'].split(':')[2])
    del object
    urls = object['twitter_entities']['urls']
    query = 'INSERT INTO urls (idtweet, url, expanded_url, display_url, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s)'
    object = []
    for url in urls:
        object.append((idtweet, url['url'], url['expanded_url'], url['display_url'], url['indices'][0],
                       url['indices'][1]))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_mentions(object):
    idtweet = int(object['id'].split(':')[2])
    del object
    mentions = object['twitter_entities']['user_mentions']
    query = 'INSERT INTO mentions (idtweet, screen_name, name, user_id, start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s)'
    object = []
    for mention in mentions:
        object.append((idtweet, mention['screen_name'], mention['name'], mention['id'], mention['indices'][0],
                       mention['indices'][1]))
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_media(object):
    idtweet = int(object['id'].split(':')[2])
    del object
    media = object['twitter_entities']['media']
    query = 'INSERT INTO media (idmedia, idtweet, media_url, media_url_https, url, display_url, expanded_url, type, ' \
            ' start_index, end_index)'
    params = 'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    object = []
    for medium in media:
        object.append((medium['id'], idtweet, medium['media_url'], medium['media_url_https'], medium['url'],
                       medium['display_url'], medium['expanded_url'], medium['type'], medium['indices'][0],
                       medium['indices'][1]))
        insert_media_sizes(medium['sizes'], idtweet)
    try:
        cursor.executemany(query + params, object)
    except mysql.connector.ProgrammingError as pe:
        print(pe.msg)
    except mysql.connector.DatabaseError as de:
        print(de.msg)


def insert_media_sizes(object, id):
    query = 'INSERT INTO media_sizes (idmedia, size, width, height, resize)'
    params = 'VALUES (%s, %s, %s, %s, %s)'
    temp = []
    for key in object.keys():
        temp.append((str(id), key, object[key]['width'], object[key]['height'], object[key]['resize']))
    try:
        cursor.execute(query + params, temp)
    except mysql.connector.ProgrammingError as err:
        print(err.msg)
    except mysql.connector.DatabaseError as err:
        print(err.msg)


def format_object(json_tweet):

    pass


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
                        insert_actor_links(json_tweet)
                        insert_actor_lang(json_tweet)
                        insert_object(json_tweet)
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
                        cnx.commit()
    except UnicodeDecodeError as er:
        print(er)


for file in os.listdir(directory):
    filepath = os.path.join(directory, file)
    unpack_json(filepath)
