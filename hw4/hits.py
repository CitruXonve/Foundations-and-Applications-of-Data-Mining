from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
import re
import sys
from time import time


def worker(input_file, iteration, partition=4):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    sc = spark.sparkContext
    rdd = sc.textFile(input_file, partition)

    links = rdd.map(lambda line: tuple(line.split())).distinct().map(lambda (node1, node2): (node1, [node2]))\
        .reduceByKey(add)

    reverse_links = rdd.map(lambda line: tuple(line.split())).distinct().map(lambda (node1, node2): (node2, [node1]))\
        .reduceByKey(add)

    # ranks_h = links.map(lambda x: (x[0], 1.0))
    ranks_h = rdd.flatMap(lambda line: line.split()).distinct().map(lambda x: (x, 1.0))
    result_h = ranks_h.collectAsMap()
    ranks_a = links.join(ranks_h).values().flatMap(lambda (adj_list, val): [
        (node, val) for node in adj_list]).reduceByKey(add)
    max_rank = ranks_a.values().max()
    ranks_a = ranks_a.map(lambda (node, val): (node, val/max_rank))
    result_a = ranks_a.collectAsMap()

    # print(ranks_a.sortByKey().collect())

    for iter in range(iteration):
        ranks_h = reverse_links.join(ranks_a).values().flatMap(lambda (adj_list, val): [
            (node, val) for node in adj_list]).reduceByKey(add)
        max_rank = ranks_h.values().max()
        ranks_h = ranks_h.mapValues(lambda val: val/max_rank)
        if iter == iteration-1:
            result_h = ranks_h.collectAsMap()
        # print(ranks_h.sortByKey().collect())

        ranks_a = links.join(ranks_h).values().flatMap(lambda (adj_list, val): [
            (node, val) for node in adj_list]).reduceByKey(add)
        max_rank = ranks_a.values().max()
        ranks_a = ranks_a.mapValues(lambda val: val/max_rank)
        if iter == iteration-1:
            result_a = ranks_a.collectAsMap()
        # print(ranks_a.sortByKey().collect())

    with open('p2.txt', 'w') as fout:
        # print(ranks_h.sortByKey().collect())
        
        for node in sorted(rdd.flatMap(lambda line: line.split()).distinct().collect()):
            if not node in result_h:
                result_h[node] = '0'
            if not node in result_a:
                result_a[node] = '0'

        fout.write(' '.join(['0' if val=='0' else '%.2f' % val
                             for node, val in sorted(result_h.items())])+'\n')

        # print(ranks_a.sortByKey().collect())
        fout.write(' '.join(['0' if val=='0' else '%.2f' % val
                             for node, val in sorted(result_a.items())])+'\n')
        fout.close()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Invalid arguments!")
        exit(-1)
    start_time = time()
    worker(input_file=sys.argv[1], iteration=int(sys.argv[2]))
    print('Duration:', time()-start_time)
