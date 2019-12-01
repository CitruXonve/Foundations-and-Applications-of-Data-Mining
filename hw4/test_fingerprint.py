from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee
import re
import unicodedata
import json

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .master("local[*]") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = 8

with open("clusters_facility_name.json", 'r') as fin:
    obj = json.loads(fin.readline())
    print(obj.keys())
    print(type(obj['clusters']), obj['clusters'][:2])

    sc = spark.sparkContext
    rdd = sc.parallelize(obj['clusters'], partition)

    fin.close()


with open("correct_clusters.txt", 'w') as fout:
    name_sorter = lambda d: (-d['c'], d['v'])
    for line in rdd.map(lambda d: (d['value'],map(lambda d: '%s(%d)' % (d['v'], d['c']), sorted(d['choices'], key=name_sorter))))\
        .sortByKey().filter(lambda (n,l):len(l)>0)\
        .map(lambda (n, l): n+':'+','.join(l))\
        .collect():
        fout.write(str(line)+'\n')
    fout.close()