from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee
import re
import unicodedata

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()
    # .setMaster("local[*]") \
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = 8

rdd = spark.read.csv("./dataset/steam.csv", 
                               header=True).rdd.map(tuple).repartition(partition)

print(rdd.count())

def token_fingerprint(name):
    return ''.join(sorted(set(
        re.split(r'[\p{Punct}\[\]\'\-\:\s\x00-\x1f\x7f-\x9f]+', #'\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]'
            name.lower()#unicodedata.normalize('NFKD', name.lower()).encode('ascii', 'ignore')
            )
        )))
    pass

def n_gram_fingerprint(name, size):
    clean_name = re.sub(r'[\p{Punct}\[\]\'\-\:\s\x00-\x1f\x7f-\x9f]+', #'\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]'
            '',
            name.lower()#unicodedata.normalize('NFKD', name.lower()).encode('ascii', 'ignore')
            )
            
    return ''.join(sorted(set(
            [clean_name[i:i+size] for i in range(len(clean_name)-size+1)]
        )))
    pass

app_names = rdd.map(lambda line: (token_fingerprint(line[1]), 1)).reduceByKey(add).sortBy(lambda x:(-x[1], x[0]))#.keys()

print(app_names.count(), app_names.take(5))

app_names = rdd.map(lambda line: (n_gram_fingerprint(line[1], 5), 1)).reduceByKey(add).sortBy(lambda x:(-x[1], x[0]))#.keys()

print(app_names.count(), app_names.take(5))