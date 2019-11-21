#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
from __future__ import print_function

import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession
from operator import add, __or__
from time import time

LAMBDA = 0.01   # regularization
# np.random.seed([2,4,1,3])
np.random.seed(24)


def rmse(R, ms, us, M, U):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
#
    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T
#
    for j in range(ff):
        XtX[j, j] += LAMBDA * uu
#
    # print('i=', i, 'mat=', mat, 'ratings=', ratings, 'XtX=', XtX, 'Xty=', Xty, 'solve=',np.linalg.solve(XtX, Xty))
    return np.linalg.solve(XtX, Xty)

def worker(input_file='ratings.csv', output_file='predictions.csv', partition=4):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    ratings = spark.read.csv(input_file,
                             header=True).rdd.map(lambda line: ((int(line[0]), int(line[1])), (float(line[2])-0.49)/4.61)).repartition(partition)
    ratings_mapping = ratings.collectAsMap()
    # print(ratings.map(lambda x:x[1]).max(), ratings.map(lambda x:x[1]).min())

    users = ratings.map(lambda line:(line[0][0], 1)).reduceByKey(add).keys()
    users_list = users.collect()
    users_id = dict((users_list[i], i) for i in range(users.count()))
    # users_id_reverse = dict((i, users_list[i]) for i in range(users.count()))
    movies = ratings.map(lambda line:(line[0][1], 1)).reduceByKey(add).keys()
    movies_list = movies.collect()
    movies_id = dict((movies_list[i], i) for i in range(movies.count()))
    # movies_id_reverse = dict((i, movies_list[i]) for i in range(movies.count()))

    # print(ratings_mapping.items()[:5], users_id.items()[:5], movies_id.items()[:5])
    # return
    # M = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    # U = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    # F = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    # ITERATIONS = int(sys.argv[4]) if len(sys.argv) > 4 else 5
    # partitions = int(sys.argv[5]) if len(sys.argv) > 5 else 1
    M = users.count()
    U = movies.count()
    F = 5
    ITERATIONS = 20

    # print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
    #       (M, U, F, ITERATIONS, partition))

    R = matrix(rand(M, F)) * matrix(rand(U, F).T)
    ms = matrix(rand(M, F))
    us = matrix(rand(U, F))

    def passer(key_value):
        user_id, movie_id = users_id.get(key_value[0][0]), movies_id.get(key_value[0][1])
        R[user_id, movie_id] = key_value[1]
        # print(user, movie, key_value[1])
        pass

    ratings_copy = ratings.map(passer).count()

    # print(R)
    # return
    def als(R, ms, us):
        # spark = SparkSession\
        # .builder\
        # .appName("PythonALS")\
        # .getOrCreate()

        sc = spark.sparkContext

        Rb = sc.broadcast(R)
        msb = sc.broadcast(ms)
        usb = sc.broadcast(us)

        for i in range(ITERATIONS):
            ms = sc.parallelize(range(M), partition) \
                .map(lambda x: update(x, usb.value, Rb.value)) \
                .collect()
            # collect() returns a list, so array ends up being
            # a 3-d array, we take the first 2 dims for the matrix
            ms = matrix(np.array(ms)[:, :, 0])
            msb = sc.broadcast(ms)

            us = sc.parallelize(range(U), partition) \
                .map(lambda x: update(x, msb.value, Rb.value.T)) \
                .collect()
            us = matrix(np.array(us)[:, :, 0])
            usb = sc.broadcast(us)

            error = rmse(R, ms, us, M, U)
            print("Iteration %d:" % i)
            print("RMSE: %5.4f" % error)
        
        spark.stop()
        return R, ms, us, error

    R, ms, us, __ = als(R, ms, us)
    # print(R.shape, ms.shape, us.shape)
    # print(R, ms, us.T, ms*us.T)
    # print(type(R))

    # print(np.max(R), np.min(R))
    max_r, min_r = np.max(R), np.min(R)
    with open(output_file, 'w') as fout:
        fout.write('userId,movieId,rating\n')
        sorted_users = sorted(users_id.items())
        sorted_movies = sorted(movies_id.items())
        for user, user_id in sorted_users:
            for movie, movie_id in sorted_movies:
                if not (user, movie) in ratings_mapping:
                    fout.write('%d,%d,%s\n' % (user, movie, str((R[user_id, movie_id]-min_r)/(max_r-min_r)*4.50+0.49)))
        fout.close()
        pass


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid arguments!")
        exit(-1)
    start_time = time()
    worker(input_file=sys.argv[1], output_file=sys.argv[2])
    print('Duration:', time()-start_time)
    
    
