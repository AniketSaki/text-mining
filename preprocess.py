import re
import string
from nltk.corpus import stopwords
sw = set(stopwords.words('english'))


def hashtag(text):
    return re.sub('#[a-zA-Z0-9]+', '', text)


def mention(text):
    return re.sub('@[a-zA-Z0-9_]+', '', text)


def url(text):
    return re.sub('http[s]?:[\S]+', '', text)


def punctuation(text):
    exclude = set(string.punctuation)
    return ''.join(ch for ch in text if ch not in exclude)


def stopword(text):
    return [i for i in text.lower().split() if i not in sw]
