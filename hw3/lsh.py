from pyspark.sql import SparkSession
from operator import add, __or__
from itertools import combinations
import sys
from time import time


def worker(input_file, output_file, partition=8):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    ratings = spark.read.csv(input_file,
                             header=True).rdd.map(tuple).repartition(partition)

    rating_mapping = ratings.map(lambda x: (
        (int(x[0]), int(x[1])), [float(x[2])])).reduceByKey(add).collectAsMap()

    user_movies = ratings.map(lambda line: (
        int(line[0]), {int(line[1])})).reduceByKey(__or__)
    user_movie_mapping = user_movies.collectAsMap()

    hash_num = 50
    row, band = 5, 10

    def multi_hash(key_values):
        def hash_func(x, i): return (3*x + 11*i) % 100+1
        for key, values in key_values:
            yield (key, [min(map(lambda val: hash_func(val, i), values)) for i in range(1, hash_num+1)])

    def banding(key_values):
        for key, values in key_values:
            for b in range(band):
                yield (tuple(values[b*row:(b+1)*row] + [-b]), {key})

    signatures = user_movies.mapValues(list).mapPartitions(multi_hash)

    similar_users = signatures.mapPartitions(banding).reduceByKey(
        __or__).filter(lambda x: len(x[1]) > 1)

    similar_users = similar_users.values().flatMap(lambda x: combinations(
        sorted(x), 2)).map(lambda x: (x, -1)).reduceByKey(add).keys()

    def jaccard(user_pairs):
        for user1, user2 in user_pairs:
            set1 = user_movie_mapping.get(user1, set())
            set2 = user_movie_mapping.get(user2, set())
            if len(set1 | set2) > 0:# and len(set1 & set2) > 0:
                yield (user1, [(len(set1 & set2) * 100.0/len(set1 | set2), user2)])
                yield (user2, [(len(set1 & set2) * 100.0/len(set1 | set2), user1)])

    top_3_similar_user = similar_users.mapPartitions(jaccard).reduceByKey(add)\
        .mapValues(lambda values: [val[1] for val in sorted(values, reverse=True)[:3]])

    def rating_generator(pairs):
        for user1, similar_user in pairs:
            movie_to_predict = set()
            for user in similar_user:
                movie_to_predict |= user_movie_mapping.get(user, set())
            movie_to_predict -= user_movie_mapping.get(user1, set())
            for movie in movie_to_predict:
                rating_for_this_movie = []
                for user in similar_user:
                    rating_for_this_movie += rating_mapping.get((user, movie), [])
                if len(rating_for_this_movie) > 0:
                    yield(user1, movie, sum(rating_for_this_movie)*1.0/len(rating_for_this_movie))

    prediction = top_3_similar_user.mapPartitions(
        rating_generator).sortBy(lambda x: (x[0], x[1], -x[2]))

    with open(output_file, 'w') as fout:
        fout.write('userId,movieId,rating\n')
        for line in prediction.collect():
            fout.write(','.join(map(str, line))+'\n')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Invalid arguments!")
        exit(-1)
    start_time = time()
    worker(input_file=sys.argv[1], output_file=sys.argv[2])
    print('Duration:', time()-start_time)
