from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee, combinations
import sys
from time import time

def worker(input_file, threshold=200, partition=4, confidence=0.5):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate() 
        #.config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')
    
    # Tuning

    if threshold<80:
        partition = 2
    elif threshold<100:
        partition = 1
    elif threshold<200:
        partition = 2

    support = threshold*0.98/partition
    # support = threshold*0.99/partition if threshold<100 else threshold*0.98/partition

    ratings = spark.read.csv(input_file, 
                                header=True).rdd.map(tuple).repartition(partition)

    baskets = ratings.map(lambda line: (line[0], {int(line[1])})).reduceByKey(__or__)

    print(threshold, partition, support, baskets.count(), baskets.values().getNumPartitions())

    def mapper1(iterable):
        frequency = {}
        singletons = []
        iterable, iter_baskets = tee(iterable)
        ops=[0,0,0,0,0]
        for basket in iter_baskets:
            for item in basket:
                frequency[item] = frequency.get(item, 0) + 1
                ops[0]+=1
                if frequency[item]-1 < support and frequency[item] >= support:
                    singletons.append(item)
                    yield (tuple({item}), 1)
        
        candidates = set()
        for can1, can2 in combinations(singletons, 2):
            ops[1]+=1
            candidates.add(tuple(sorted({can1, can2})))

        resources = set()
        size = 1
        while len(candidates)>0:
            
            size+=1
            resources.clear()
            iterable, iter_baskets = tee(iterable)
            for basket in iter_baskets:
                for itemtuple in candidates:
                    ops[2]+=1
                    if set(itemtuple).issubset(basket):
                        frequency[itemtuple] = frequency.get(itemtuple, 0) + 1
                        if frequency[itemtuple]-1 < support and frequency[itemtuple] >= support:
                            resources.add(itemtuple)
                            yield (itemtuple, 1)
            # print(len(candidates), size, len(candidates)*size*4.0/1024/1024)
            candidates.clear()

            for can1, can2 in combinations(resources, 2):
                ops[3]+=1
                itemtuple = tuple(sorted(set(can1) | set(can2)))
                if len(itemtuple) == size+1 and not(itemtuple in candidates):
                    is_negative = False
                    for subset in combinations(itemtuple, size):
                        if subset not in resources:
                            is_negative = True
                            break

                    if not is_negative:
                        candidates.add(itemtuple)

            pass    

        print(ops)
        pass

    def printer(iterable):
        for x in iterable:
            print x
        print("--")
        yield None

    phase1 = baskets.values().mapPartitions(mapper1).reduceByKey(add)

    baskets_set = baskets.values().map(tuple).collect()

    def mapper2(iterable):
        frequency = {}

        for item in iterable:
            for basket in baskets_set:
                if set(item).issubset(basket):
                    frequency[item] = frequency.get(item, 0) + 1

        for item, count in frequency.items():
            yield (tuple(sorted(item)), count)
        pass

    phase2 = phase1.keys().mapPartitions(mapper2).reduceByKey(add).filter(lambda x:x[1]>=threshold).persist()

    frequents = phase2.keys().collect()

    frequencies = phase2.collectAsMap()

    def mapper3(iterable):
        for item in iterable:
            subset = tuple(set(iterable) - {item})
            ratio = frequencies.get(iterable, 0.0)*1.0/frequencies.get(subset) if frequencies.has_key(subset) else None
            if ratio is not None and ratio >= confidence:
                # print(subset, item ,frequencies.get(iterable, 0.0),frequencies.get(subset))
                yield((subset, item), ratio)


    asso_rules = phase2.keys().repartition(partition*4).flatMap(mapper3).collect()

    with open('output.txt', 'w') as fout:
        fout.write('Frequent itemset\n')
        for itemtuple in sorted(frequents):
            fout.write(','.join(map(str, itemtuple))+'\n')
        fout.write('Confidence\n')
        for rule in sorted(asso_rules):
            fout.write('(%s),%d %s\n' % (','.join(map(str, rule[0][0])), rule[0][1], str(rule[1])))
        pass

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Invalid arguments!")
        exit(-1)
    start_time = time()
    worker(input_file=sys.argv[1], threshold=int(sys.argv[2]), confidence=float(sys.argv[3]))
    print('Duration:',time()-start_time)