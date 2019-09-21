import os
import pandas
import json

delimiter = '__'
df = pandas.DataFrame()
count = 0
total = 5757
directory = os.fsencode('C:/Users/Saki/Desktop/PTDataDownload/PTDataDownload/json/')
store = pandas.HDFStore('C:/Users/Saki/Desktop/PTDataDownload/PTDataDownload/json/store.h5')


def flatten_json(parent):
    val = {}
    for parent_key in parent.keys():
        if isinstance(parent[parent_key], dict):
            child = flatten_json(parent[parent_key])
            for child_key in child.keys():
                val[parent_key + delimiter + child_key] = child[child_key]
        elif isinstance(parent[parent_key], list):
            for i in range(0, len(parent[parent_key])):
                if isinstance(parent[parent_key][i], dict):
                    child = flatten_json(parent[parent_key][i])
                    for child_key in child.keys():
                        val[parent_key + delimiter + str(i) + delimiter + child_key] = child[child_key]
                else:
                    val[parent_key + delimiter + str(i)] = parent[parent_key][i]
        else:
            val[parent_key] = parent[parent_key]
    return val


def add_to_df(filename):
    json_list = []
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
                    flat_json = flatten_json(json_tweet)
                    json_list.append(flat_json)
    return json_list


for file in os.listdir(directory):
    filepath = os.path.join(directory, file)
    try:
        jsons = add_to_df(filepath)
        for tweet in jsons:
            temp = pandas.DataFrame({'value': tweet}).transpose()
            df = df.append(temp)
    except UnicodeDecodeError as er:
        print(er)
    count += 1
    print((count/total)*100)
store['df'] = df
print(store)
store.close()
