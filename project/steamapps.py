from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee
import re
import unicodedata
import string

spark = SparkSession \
    .builder \
    .appName("Python Spark") \
    .getOrCreate()
    # .setMaster("local[*]") \
    #.config("spark.some.config.option", "some-value") \
spark.sparkContext.setLogLevel('ERROR')

partition = 8

rdd = spark.read.csv("./dataset/steam.csv", 
                               header=True).rdd.map(tuple).repartition(partition).map(lambda line: (line[1], 1)).reduceByKey(add).keys()

print(rdd.count())

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
        return (output, [name])
    else:
        return (translate(name).lower(), [name])
    pass

def n_gram_fingerprint(name, size):
    clean_name = re.sub('['+string.punctuation+'\x00-\x1f\x7f-\x9f]+',
            '',
            translate(name).lower()
            )
            
    output = ' '.join(sorted(set(
            [clean_name[i:i+size] for i in range(len(clean_name)-size+1)] if len(clean_name)>size else [clean_name]
        )))

    # return (output, [name])
    if len(output)>0:
        return (output, [name])
    else:
        return (translate(name).lower(), [name])
    pass

app_names = rdd.map(token_fingerprint).filter(lambda (finger, __): len(finger)>0).reduceByKey(add).sortBy(lambda x:(-len(x[1]), x[0]))#.keys()

print(app_names.count(), app_names.filter(lambda x:len(x[1])>1).count(), app_names.take(5))

with open('steam_token.csv', 'w') as fout:
    fout.write('fingerprint,names\n')
    for f, n in app_names.collect():
        fout.write(u'{},{}\n'.format(f, ';'.join(n)).encode('utf-8'))
    fout.close()

app_names = rdd.map(lambda line: n_gram_fingerprint(line, 6)).filter(lambda (finger, __): len(finger)>0).reduceByKey(add).sortBy(lambda x:(-len(x[1]), x[0]))#.keys()

print(app_names.count(), app_names.filter(lambda x:len(x[1])>1).count(), app_names.take(5))

with open('steam_n_gram.csv', 'w') as fout:
    fout.write('fingerprint,names\n')
    for f, n in app_names.collect():
        fout.write(u'{},{}\n'.format(f, ';'.join(n)).encode('utf-8'))
    fout.close()