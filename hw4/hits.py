from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee
import re
import unicodedata

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .master("local[*]") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = None

sc = spark.sparkContext
rdd = sc.textFile("graph.txt", partition)

links = rdd.map(lambda line:tuple(line.split())).distinct().map(lambda (node1, node2): (node1, [node2]))\
    .reduceByKey(add)

reverse_links = rdd.map(lambda line:tuple(line.split())).distinct().map(lambda (node1, node2): (node2, [node1]))\
    .reduceByKey(add)

ranks_h = links.map(lambda x: (x[0], 1.0))
ranks_a = links.join(ranks_h).values().flatMap(lambda (adj_list, val): [(node, val) for node in adj_list]).reduceByKey(add)
max_rank = ranks_a.values().max()
ranks_a = ranks_a.map(lambda (node, val): (node, val/max_rank))

print(ranks_a.sortByKey().collect())

iteration = 10

for iter in range(iteration):
    ranks_h = reverse_links.join(ranks_a).values().flatMap(lambda (adj_list, val): [(node, val) for node in adj_list]).reduceByKey(add)
    max_rank = ranks_h.values().max()
    ranks_h = ranks_h.mapValues(lambda val: val/max_rank)
    print(ranks_h.sortByKey().collect())

    ranks_a = links.join(ranks_h).values().flatMap(lambda (adj_list, val): [(node, val) for node in adj_list]).reduceByKey(add)
    max_rank = ranks_a.values().max()
    ranks_a = ranks_a.mapValues(lambda val: val/max_rank)
    print(ranks_a.sortByKey().collect())

with open('h_a.txt', 'w') as fout:
    result = {}
    for node in sorted(rdd.flatMap(lambda line:line.split()).distinct().collect()):
        result[node] = '0'
    for node, value in ranks_h.collect():
        result[node] = value
    fout.write(' '.join([str(val) for node, val in sorted(result.items())])+'\n')
    
    result = {}
    for node in sorted(rdd.flatMap(lambda line:line.split()).distinct().collect()):
        result[node] = '0'
    for node, value in ranks_a.collect():
        result[node] = value
    fout.write(' '.join([str(val) for node, val in sorted(result.items())])+'\n')
    fout.close()