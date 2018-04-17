import pickle

import mysql.connector
import numpy
from gensim import models, matutils

from Code.preprocess import *


def update_matrix(i, j):
    global counter
    structure.append([cluster_set[str(idcluster[i])], cluster_set[str(idcluster[j])], cos_sim[i][j]])
    idcluster.append([idcluster[i], idcluster[j]])
    cluster_set[str(idcluster[-1])] = counter
    counter += 1
    vectors.append(matutils.unitvec(numpy.mean([vectors[i], vectors[j]], axis=0)))
    if i > j:
        for k in range(j + 1, i):
            del cos_sim[k][j]
        for k in range(i + 1, len(cos_sim) - 1):
            del cos_sim[k][i]
            del cos_sim[k][j]
    else:
        for k in range(i + 1, j):
            del cos_sim[k][i]
        for k in range(j + 1, len(cos_sim) - 1):
            del cos_sim[k][j]
            del cos_sim[k][i]
    del idcluster[i], idcluster[j], cos_sim[i], cos_sim[j], vectors[i], vectors[j]
    for k in range(0, len(vectors) - 1):
        cos_sim[-1].append(numpy.dot(vectors[-1], vectors[k]))
    cos_sim.append([])


# initiate variables
vectors = []
cluster_set = {}
idcluster = []
structure = []
cos_sim = [[]]
model = models.KeyedVectors.load_word2vec_format(
    '..\Resources\GoogleNews-vectors-negative300.bin\GoogleNews-vectors-negative300.bin', binary=True)
user = 'aniket'
password = 'aniket'
database = 'twitter'
host = '127.0.0.1'
cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
cursor = cnx.cursor()
query = "SELECT idtweet, body FROM tweet WhERE twitter_lang='en'"
cursor.execute(query)
counter = 0

# store idtweet and calculate cosine similarity matrix
for record in cursor:
    tweet = record[1].decode('utf-8')
    tweet = hashtag(tweet)
    tweet = mention(tweet)
    tweet = url(tweet)
    tweet = punctuation(tweet)
    words = stopword(tweet)
    v1 = [model[word] for word in words if word in model.vocab]
    if len(v1) > 0:
        idcluster.append([record[0]])
        cluster_set[str(idcluster[-1])] = counter
        vectors.append(matutils.unitvec(numpy.mean(v1, axis=0)))
        for j in range(0, len(vectors) - 1):
            cos_sim[counter].append(numpy.dot(vectors[counter], vectors[j]))
        counter += 1
        cos_sim.append([])
    print('progress: ', (counter / 5912319) * 100)
# store to file
with open('../Resources/cos_sim.pkl', 'wb') as wf:
    pickle.dump(cos_sim, wf)
with open('vectors.pkl', 'wb') as wf:
    pickle.dump(vectors, wf)
print('-------------------------------------------------------------------------------------------------------------')
print('clustering...')
# get max value from dot product
lc = len(cos_sim)
while len(cos_sim) > 2:
    i = numpy.argmax(cos_sim)
    j = numpy.argmax(cos_sim[i])
    update_matrix(i, j)
    print('progress: ', (len(cos_sim) / lc) * 100)

with open('../Resources/cluster.pkl', 'wb') as wf:
    pickle.dump(idcluster, wf)
with open('../Resources/structure.pkl', 'wb') as wf:
    pickle.dump(structure, wf)
with open('../Resources/cluster_set.pkl', 'wb') as wf:
    pickle.dump(cluster_set, wf)

cursor.close()
cnx.close()
