from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee, combinations
import sys
from time import time

def worker(input_file, threshold=200, partition=2, confidence=0.5):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .getOrCreate()
        # .setMaster("local[*]") \
        #.config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    support = threshold*0.9/partition

    print(threshold, partition, support)

    ratings = spark.read.csv(input_file, 
                                header=True).rdd.map(tuple).repartition(partition)

    baskets = ratings.map(lambda line: (line[0], {int(line[1])})).reduceByKey(__or__)

    print(baskets.count(), baskets.values().getNumPartitions())

    def mapper1(iterable):
        frequency = {}
        singletons = []
        candidates = Queue()
        iterable, iter_baskets = tee(iterable)
        ops=[0,0,0,0,0]
        for basket in iter_baskets:
            for item in basket:
                frequency[item] = frequency.get(item, 0) + 1
                ops[0]+=1
                # if frequency[item] >= support:
                #     singletons.append(frozenset([item]))
                #     candidates.put(frozenset([item]))
                    # yield(item,1)
        
        negatives = set()
        for item, count in frequency.items():
            ops[1]+=1
            if count>=support:
                singletons.append({item})
                candidates.put({item})
                yield (tuple({item}), 1)
        # print(singletons, candidates)
        # return
        # frequency.clear()

        while not(candidates.empty()):
            item = candidates.get()
            count = frequency.get(tuple(item), 0)

            if frequency.get(tuple(item), None) is None:
                iterable, iter_baskets = tee(iterable)
                for basket in iter_baskets:
                    ops[2]+=1
                    if item.issubset(basket):
                        count+=1
                frequency[tuple(item)] = count

            if count>=support:
                # yield (tuple(item), 1)
                yield (tuple(sorted(item)), 1)
                # print(item)
                for single in singletons:
                    ops[3]+=1
                    if not(single in item):# and len(item)<3:
                        new_item = item | single
                        if tuple(new_item) in frequency:
                            continue
                        is_negative = False
                        # for nega in negatives:
                        #     if set(nega).issubset(new_item):
                        #         iter_baskets = True
                        for subset in combinations(new_item, len(item)):
                            ops[4]+=1
                            if tuple(subset) in negatives:
                                is_negative = True
                        if not is_negative:
                            candidates.put(item.union(single))
                    pass
            else:
                negatives.add(tuple(item))
            pass    
        # output = []
        # while not candidates.empty():
        #     output.append(candidates.get())
        # print(output)
        # return
        
        # for item in frequency.items():
        #     if item[1]>=support:
        #         yield (item[0], 1)
        print(ops)
        pass

    def printer(iterable):
        for x in iterable:
            print x
        print("--")
        yield None

    phase1 = baskets.values().mapPartitions(mapper1).reduceByKey(add)

    # print(phase1.take(5), phase1.getNumPartitions(), phase1.keys().take(5))
    # print(phase1.keys().collect(), phase1.count())
    # exit(0)
    baskets_set = baskets.values().map(tuple).collect()
    # print(baskets_set[:5])

    def mapper2(iterable):
        frequency = {}

        for item in iterable:
            for basket in baskets_set:
                if set(item).issubset(basket):
                    frequency[item] = frequency.get(item, 0) + 1

        for item, count in frequency.items():
            yield (tuple(sorted(item)), count)
        pass

    phase2 = phase1.keys().mapPartitions(mapper2).reduceByKey(add).filter(lambda x:x[1]>=threshold)

    # print(phase2.take(5))

    frequents = phase2.keys().collect()

    with open('output.txt', 'w') as fout:
        # output_buffer = []
        # for itemset in frequents:
        #     output_buffer.append(','.join(map(str, itemset))+'\n')
        # for line in sorted(output_buffer):
        #     fout.write(line)
        for itemset in sorted(frequents):
            fout.write(','.join(map(str, itemset))+'\n')
        pass

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Invalid arguments!")
        exit(-1)
    start_time = time()
    worker(input_file=sys.argv[1], threshold=int(sys.argv[2]), confidence=float(sys.argv[3]))
    print('Duration:',time()-start_time)