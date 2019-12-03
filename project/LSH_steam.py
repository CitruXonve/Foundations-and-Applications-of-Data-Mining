from pyspark import SparkContext
import numpy as np
import sys
import hashlib
import pandas as pd 

def get_dataframe(file):
	"""
		get dataframe and name column
	"""
	df = pd.read_csv(file)
	name = df['name']
	name_list = name.unique()
	name_list = name_list.tolist()

	return name_list

def make_basket(name_list):

	total_name = len(name_list)
	name_basket = [[] for i in range(200)]
	point = 0
	curr_basket = 0

	while point < total_name:
		if curr_basket == len(name_basket):
			curr_basket = 0	
		name_basket[curr_basket].append(name_list[point])
		curr_basket += 1
		point += 1
	return name_basket

def minhash(iterable):
	
	steam_game = []
	for i in iterable:
		steam_game.append(i) 

	game_num = len(steam_game)
	
	game_set_table = np.zeros((name_num, game_num))

	game_set = np.empty(game_num, dtype=int)
	print(type(steam_game), type(steam_game[0]), str(steam_game[0])[:30])
	exit(0)
	for i, (name, game_basket) in enumerate(steam_game):
		user_set[i] = user
		movie_set_table[:,i] = movie_transformation(movie_set, game_basket)
	
	s = 50
	signature_table = np.empty((game_num, s))
	
	for i in range(s):
		signature_table[:,i] =  hashlib.md5(game_set.encode())
	
	signature_matrix = np.full((s, game_num), 101)
	
	for i in range(name_num):
		game_id = game_set[i]
		game_signatures = signature_table[i]
		
		for j in range(m):
			if game_set_table[i,j] == 1:
				for k in range(s):
					if signature_matrix[k,j] > game_signatures[k]:
						signature_matrix[k,j] = game_signatures[k]            
	return [[game_set, signature_matrix]]

def LSH(iterable):
    band = np.empty((0,game_num))
    for i in iterable:
        band = np.vstack((band, i))
    s = band.shape[0]  # signature num
    
    identical_pairs = []
    for i in range(game_num):
        for j in range(i+1,game_num):
            if all(band[:,i] == band[:,j]):
                identical_pairs.append((game_set[i], game_set[j]))
    return identical_pairs



if __name__ == "__main__":

	file = sys.argv[1]
	name_list = get_dataframe(file)
	name_basket = make_basket(name_list)

	sc = SparkContext()
	name_set = sc.parallelize(name_basket).map(lambda x: x[1]).flatMap(list).distinct().collect()
	name_set = np.array(name_set)

	name_set.sort()
	name_num = len(name_set)

	minhash_result = sc.parallelize(name_basket).mapPartitions(minhash).collect()
	
	game_set = minhash_results[0][0]
	signature_matrix = minhash_results[0][1]

	for i,result in enumerate(minhash_results):
		if i > 0:
			game_set = np.append(game_set, result[0])
			signature_matrix = np.hstack((signature_matrix, result[1]))

	game_num = signature_matrix.shape[1]  # user count
	
	# get band for LSH
	band_num = 10
	band_sets = sc.parallelize(signature_matrix, band_num)

	# LSH
	LSH_results = band_sets.mapPartitions(LSH).collect()

	similar_user_pairs = sorted(list(set(LSH_results)))

	# save for dictionary
	name_dict = name_baskets.collectAsMap()
	for name in name_dict:
		name_dict[name] = set(name_dict[name])

