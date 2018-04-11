import pickle
import mysql.connector
import numpy
from gensim import models, matutils

from Code.preprocess import *


def update_matrix(i, j):
    ids.append([ids[i], ids[j]])
    vectors.append(matutils.unitvec(numpy.mean([vectors[i], vectors[j]], axis=0)))
    del ids[i], ids[j], cos_sim[i], cos_sim[j], vectors[i], vectors[j]
    cos_sim.append([])
    for i in range(0, len(vectors) - 1):
        cos_sim[-1].append(numpy.dot(vectors[-1], vectors[i]))


# initiate variables
vectors = []
ids = []
cos_sim = [[]]
vocab = [0, 0]
model = models.KeyedVectors.load_word2vec_format(
    '.\Resources\GoogleNews-vectors-negative300.bin\GoogleNews-vectors-negative300.bin', binary=True)
user = 'aniket'
password = 'aniket'
database = 'twitter'
host = '127.0.0.1'
cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
cursor = cnx.cursor()
query = "select idtweet, body from tweet where twitter_lang='en'"
cursor.execute(query)
i = 0

# store tokens and calculate cosine similarity matrix
for record in cursor:
    ids.append([record[0]])
    tweet = record[1]
    tweet = hashtag(tweet)
    tweet = mention(tweet)
    tweet = url(tweet)
    tweet = punctuation(tweet)
    words = stopword(tweet)
    v1 = [model[word] for word in words if word in model.vocab]
    vectors.append(matutils.unitvec(numpy.mean(v1, axis=0)))
    for j in range(0, len(vectors) - 1):
        cos_sim[i].append(numpy.dot(vectors[i], vectors[j]))
    i += 1
    cos_sim.append([])
# store to file
with open('cos_sim.pkl', 'wb') as cm:
    pickle.dump(cos_sim, cm)
with open('vectors.pkl', 'wb') as wf:
    pickle.dump(vectors, wf)

# get max value from dot product
while len(cos_sim) > 1:
    i = numpy.argmax(cos_sim)
    j = numpy.argmax(cos_sim[i])
    update_matrix(i, j)

cursor.close()
cnx.close()
