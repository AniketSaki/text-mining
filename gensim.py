from gensim import models, matutils
import mysql.connector
from Code.preprocess import *
import numpy
import pickle


def update_matrix(i, j):
    vectors.append((vectors[i] + vectors[j]) / 2)
    del cos_sim[i], cos_sim[j]
    cos_sim.append([])
    for i in range(0, len(vectors)-1):
        cos_sim[-1].append(numpy.dot(vectors[-1], vectors[i]))


# initiate variables
vectors = []
tokens = []
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
query = "select body from tweet where twitter_lang='en'"
cursor.execute(query)
i = 0

# store tokens and calculate cosine similarity matrix
for record in cursor:
    tweet = record[0]
    tweet = hashtag(tweet)
    tweet = mention(tweet)
    tweet = url(tweet)
    tweet = punctuation(tweet)
    words = stopword(tweet)
    tokens.append(words)
    v1 = [model[word] for word in words if word in model.vocab]
    vectors.append(matutils.unitvec(numpy.mean(v1, axis=0)))
    for j in range(0, len(tokens) - 1):
        cos_sim[i].append(numpy.dot(vectors[i], vectors[j]))
    i += 1
    cos_sim.append([])
# store to file
with open('cos_sim.pkl', 'wb') as cm:
    pickle.dump(cos_sim, cm)
with open('words.pkl', 'wb') as wf:
    pickle.dump(tokens, wf)

# get max value from dot product
i = numpy.argmax(cos_sim)
j = numpy.argmax(cos_sim[i])

update_matrix(i, j)
cursor.close()
cnx.close()
