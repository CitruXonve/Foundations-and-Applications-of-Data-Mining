from pyspark.sql import SparkSession
from operator import add, __or__
from itertools import combinations
from functools import reduce
import sys
from time import time
import re
import unicodedata
import string


def worker(input_file, output_file, partition=8):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    names = spark.read.csv(input_file,
                             header=True).rdd.map(tuple).repartition(partition).map(lambda line: (line[1], 1)).reduceByKey(add).map(lambda line: (line[0], token_fingerprint(line[0])))

    def convert_non_ascii_ch(ch):
        if ch in set(['u00C0', 'u00C1', 'u00C2', 'u00C3', 'u00C4', 'u00C5', 'u00E0', 'u00E1', 'u00E2', 'u00E3', 'u00E4', 'u00E5', 'u0100', 'u0101', 'u0102', 'u0103', 'u0104', 'u0105']):
            return 'a'
        if ch in set(['u00C7', 'u00E7', 'u0106', 'u0107', 'u0108', 'u0109', 'u010A', 'u010B', 'u010C', 'u010D']):
            return 'c'
        if ch in set(['u00D0', 'u00F0', 'u010E', 'u010F', 'u0110', 'u0111']):
            return 'd'
        if ch in set(['u00C8', 'u00C9', 'u00CA', 'u00CB', 'u00E8', 'u00E9', 'u00EA', 'u00EB', 'u0112', 'u0113', 'u0114', 'u0115', 'u0116', 'u0117', 'u0118', 'u0119', 'u011A', 'u011B']):
            return 'e'
        if ch in set(['u011C', 'u011D', 'u011E', 'u011F', 'u0120', 'u0121', 'u0122', 'u0123']):
            return 'g'
        if ch in set(['u0124', 'u0125', 'u0126', 'u0127']):
            return 'h'
        if ch in set(['u00CC', 'u00CD', 'u00CE', 'u00CF', 'u00EC', 'u00ED', 'u00EE', 'u00EF', 'u0128', 'u0129', 'u012A', 'u012B', 'u012C', 'u012D', 'u012E', 'u012F', 'u0130', 'u0131']):
            return 'i'
        if ch in set(['u0134', 'u0135']):
            return 'j'
        if ch in set(['u0136', 'u0137', 'u0138']):
            return 'k'
        if ch in set(['u0139', 'u013A', 'u013B', 'u013C', 'u013D', 'u013E', 'u013F', 'u0140', 'u0141', 'u0142']):
            return 'l'
        if ch in set(['u00D1', 'u00F1', 'u0143', 'u0144', 'u0145', 'u0146', 'u0147', 'u0148', 'u0149', 'u014A', 'u014B']):
            return 'n'
        if ch in set(['u00D2', 'u00D3', 'u00D4', 'u00D5', 'u00D6', 'u00D8', 'u00F2', 'u00F3', 'u00F4', 'u00F5', 'u00F6', 'u00F8', 'u014C', 'u014D', 'u014E', 'u014F', 'u0150', 'u0151']):
            return 'o'
        if ch in set(['u0154', 'u0155', 'u0156', 'u0157', 'u0158', 'u0159']):
            return 'r'
        if ch in set(['u015A', 'u015B', 'u015C', 'u015D', 'u015E', 'u015F', 'u0160', 'u0161', 'u017F']):
            return 's'
        if ch in set(['u0162', 'u0163', 'u0164', 'u0165', 'u0166', 'u0167']):
            return 't'
        if ch in set(['u00D9', 'u00DA', 'u00DB', 'u00DC', 'u00F9', 'u00FA', 'u00FB', 'u00FC', 'u0168', 'u0169', 'u016A', 'u016B', 'u016C', 'u016D', 'u016E', 'u016F', 'u0170', 'u0171', 'u0172', 'u0173']):
            return 'u'
        if ch in set(['u0174', 'u0175']):
            return 'w'
        if ch in set(['u00DD', 'u00FD', 'u00FF', 'u0176', 'u0177', 'u0178']):
            return 'y'
        if ch in set(['u0179', 'u017A', 'u017B', 'u017C', 'u017D', 'u017E']):
            return 'z'
        return ch
        pass

    def translate(name):
        return ''.join([convert_non_ascii_ch(ch) for ch in name])

    def token_fingerprint(name):
        clean_name = re.sub('['+string.punctuation+'\x00-\x1f\x7f-\x9f]+',
                '',
                translate(name).lower()
                ).split()

        output = ' '.join(sorted(set(
            clean_name
            )))

        # return (output, [name])
        if len(output)>0:
            return output
        else:
            return 'None'
        pass

    def n_gram_fingerprint(name, size):
        clean_name = re.sub('['+string.punctuation+'\x00-\x1f\x7f-\x9f]+',
                '',
                translate(name).lower()
                )
                
        output = ' '.join(sorted(set(
                [clean_name[i:i+size] for i in range(len(clean_name)-size+1)] if len(clean_name)>size else [clean_name]
            )))

        # return output
        # return (output, [name])
        if len(output)>0:
            return output
        else:
            return 'None'
        pass

    def processor(names):

        row, band = 3, 20
        hash_num = row * band

        def multi_hash(key_values):
            def hash_func(x, i): return (3*ord(x) + 11*i) % 90679+1
            for key, values in key_values:
                # yield (key, [min(map(lambda val: hash_func(val, i), values)) for i in range(1, hash_num+1)])
                yield (key, [reduce(lambda a,b: (a<<2+b) % 659+1, map(lambda val: hash_func(val, i), values)) for i in range(1, hash_num+1)])

        def banding(key_values):
            for key, values in key_values:
                for b in range(band):
                    yield (tuple(values[b*row:(b+1)*row] + [-b]), {key})

        signatures = names.mapValues(list).mapPartitions(multi_hash)

        similar_names = signatures.mapPartitions(banding).reduceByKey(
            __or__)

        singles = similar_names.filter(lambda x: len(x[1]) == 1).map(lambda x:(list(x[1])[0], 1)).reduceByKey(add).keys()

        similar_names = similar_names.values().flatMap(lambda x: combinations(
            sorted(x), 2)).map(lambda x: (min(x), {max(x)})).reduceByKey(__or__).map(lambda x:{x[0]} | x[1])
        print(similar_names.take(5))
        print(singles.take(5))
        return similar_names.collect(), singles.collect()

    with open('lsh_token.csv', 'w') as fout:
        # fout.write('userId,movieId,rating\n')
        plurals, singles = processor(names)
        for cluster in plurals:
            line = u','.join(cluster)+u'\n'
            fout.write(line.encode('utf-8'))
        fout.write(u'\n'.join(singles).encode('utf-8'))
        fout.close()
    # return
    names = spark.read.csv(input_file,
                             header=True).rdd.map(tuple).repartition(partition).map(lambda line: (line[1], 1)).reduceByKey(add).map(lambda line: (line[0], n_gram_fingerprint(line[0], 5)))
    with open('lsh_n_gram.csv', 'w') as fout:
        # fout.write('userId,movieId,rating\n')
        plurals, singles = processor(names)
        for cluster in plurals:
            line = u','.join(cluster)+u'\n'
            fout.write(line.encode('utf-8'))
        fout.write(u'\n'.join(singles).encode('utf-8'))
        fout.close()

if __name__ == '__main__':
    # if len(sys.argv) != 3:
    #     print("Invalid arguments!")
    #     exit(-1)
    start_time = time()
    worker(input_file='./steam-store-games/steam.csv', output_file='lsh.csv')
    print('Duration:', time()-start_time)
