import mysql.connector
import json
import os

user = 'aniket'
password = 'aniket'
root = 'admin'
database = 'twitter'
host = '127.0.0.1'
directory = os.fsencode('L:/PTDataDownload/json/')
cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
cursor = cnx.cursor()
add_employee = "INSERT INTO tweet (tweet_id, id_str, objectType, verb, postedTime, link, body, favoritesCount, " \
               "retweetCount, filter_level, lang, actor_id, generator_name, generator_link, provider_name, " \
               "provider_link, gnip_lang) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
               "%s, %s, %s, %s, %s)"


def insert_tweet(json_tweet):
    keys = json_tweet.keys()
    
    pass


def unpack_json(filename):
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
                    insert_tweet(json_tweet)
                    pass


for file in os.listdir(directory):
    filepath = os.path.join(directory, file)
    try:
        unpack_json(filepath)
        pass
    except UnicodeDecodeError as er:
        print(er)
