from pyspark.sql import SparkSession
from operator import add, __or__
from Queue import Queue
from itertools import tee, combinations
import sys
from time import time


def worker(input_file = 'ratings.csv', partition=4):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate() 
        #.config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    ratings = spark.read.csv(input_file, 
                                    header=True).rdd.map(tuple).repartition(partition)

    rating_mapping = ratings.map(lambda x:((int(x[0]),int(x[1])), float(x[2]))).collectAsMap()

    user_movies = ratings.map(lambda line: (int(line[0]), {int(line[1])})).reduceByKey(__or__)

    with open('user_movies.txt', 'w') as fout:
        fout.write(str(user_movies.count())+'\n')
        for line in sorted(user_movies.collect()):
            fout.write(str(line[0])+str(sorted(line[1]))+'\n') 

    def multi_hash(values):
        hash_func = lambda x,i: (3*x + 11*i)%100+1

        # signature = []
        # for i in range(50):
            # hashed_values = []
            # for val in values:
            #     hashed_values.append(hash_func(val, i))
            # signature.append(min(hashed_values))
            # print(values, hashed_values, signature)
            # return signature
        # return signature
        return [min(map(lambda val: hash_func(val, i), values)) for i in range(1, 51)]

    row, band = 5, 10

    def banding(key_values):
        key, values = key_values
        for b in range(band):
            yield (tuple(values[b*row:(b+1)*row] + [-b]), key)
        pass

    signatures = user_movies.mapValues(list).mapValues(multi_hash)

    similar_users = signatures.flatMap(banding).groupByKey().filter(lambda x:len(x[1])>1).mapValues(set).mapValues(sorted)

    with open('similar_users1_2.txt', 'w') as fout:
        fout.write(str(similar_users.count())+'\n')
        for line in similar_users.collect():
            fout.write(str(line)+'\n')

    similar_users = similar_users.values().flatMap(lambda x: combinations(x, 2)).map(lambda x:(x, -1)).groupByKey().keys()

    with open('similar_users2_2.txt', 'w') as fout:
        fout.write(str(similar_users.count())+'\n')
        for line in similar_users.collect():
            fout.write(str(line)+'\n')

    user_movie_mapping = user_movies.collectAsMap()

    def jaccard(user_pair):
        user1, user2 = user_pair
        set1 = user_movie_mapping.get(user1, set())
        set2 = user_movie_mapping.get(user2, set())
        if len(set1 | set2)>0:# and len(set1 & set2)>0:
            yield (user1, (len(set1 & set2) * 100.0/len(set1 | set2), user2))
            yield (user2, (len(set1 & set2) * 100.0/len(set1 | set2), user1))
        pass

    def sorted_by_jaccard(values):
        return [val[1] for val in sorted(values, reverse=True)[:3]]
        pass

    top_3_similar_user = similar_users.flatMap(jaccard).groupByKey().mapValues(sorted_by_jaccard)
        #.sortBy(lambda x:(-x[1], x[0]))

    with open('signatures_2.txt', 'w') as fout:
        fout.write(str(signatures.count())+'\n')
        for line in signatures.collect():
            fout.write(str(line)+'\n')

    with open('top_3_2.txt', 'w') as fout:
        fout.write(str(top_3_similar_user.count())+'\n')
        for line in sorted(top_3_similar_user.collect()):
            fout.write(str(line)+'\n')

    def aggregator(pair):
        user1, similar_user = pair
        movie_to_predict = set()
        for user in similar_user:
            movie_to_predict |= user_movie_mapping.get(user, set())
        movie_to_predict -= user_movie_mapping.get(user1, set())
        for movie in movie_to_predict:
            rating_for_this_movie = []
            for user in similar_user:
                rating = rating_mapping.get((user, movie), None)
                if rating is not None:
                    rating_for_this_movie.append(rating)
            if len(rating_for_this_movie)>0:
                yield(user1, movie, sum(rating_for_this_movie)*1.0/len(rating_for_this_movie))
        pass

    prediction = top_3_similar_user.flatMap(aggregator).sortBy(lambda x:(x[0], x[1], -x[2]))

    with open('predictions_2.csv', 'w') as fout:
        fout.write('userId,movieId,rating\n')
        for line in prediction.collect():
            fout.write(','.join(map(str, line))+'\n')
        pass

if __name__ == '__main__':
    # if len(sys.argv) != 4:
    #     print("Invalid arguments!")
    #     exit(-1)
    start_time = time()
    worker()
    print('Duration:',time()-start_time)