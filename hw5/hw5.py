from pyspark.sql import SparkSession
from operator import add, __or__
import sys
import networkx as nx


def worker(input_file, partition=None):
    spark = SparkSession \
        .builder \
        .appName("Python Spark") \
        .master("local[*]") \
        .getOrCreate()
    # .config("spark.some.config.option", "some-value") \
    spark.sparkContext.setLogLevel('ERROR')

    sc = spark.sparkContext
    rdd = sc.textFile(input_file, partition)
    initial_edges = rdd.map(lambda line: tuple(line.split(',')))
    name_mapping = initial_edges.flatMap(lambda line: [(line[0], 1), (line[1], 1)]).reduceByKey(
        add).sortByKey().keys().zipWithIndex().collectAsMap()
    reverse_name_mapping = dict([(v, k) for k, v in name_mapping.items()])

    graph = nx.Graph()
    for s, t in initial_edges.collect():
        graph.add_edge(name_mapping[s], name_mapping[t])

    def betweenness(graph, s):
        depth = {s: 0}
        for s0, s1 in list(nx.bfs_edges(graph, source=s)):
            depth[s1] = depth[s0] + 1

        indeg = {s: 0}
        for s0, s1 in graph.edges:
            if depth[s0] > depth[s1]:
                indeg[s0] = indeg.get(s0, 0) + 1
            elif depth[s0] < depth[s1]:
                indeg[s1] = indeg.get(s1, 0) + 1

        btwness = {}
        for node in graph.nodes:
            btwness[node] = 1.0
        for s0, s1 in sorted(graph.edges, key=lambda (s, t): max(depth[s], depth[t]), reverse=True):
            if depth[s0] > depth[s1]:
                s0, s1 = s1, s0
            if depth[s0] != depth[s1]:
                btwness[s0] = btwness.get(s0, 1.0) + btwness[s1]/indeg[s1]
                yield (min(s0, s1), max(s0, s1)), btwness[s1]/indeg[s1]*0.5

    results = sc.parallelize(name_mapping.values())\
        .flatMap(lambda x: betweenness(graph, x)).reduceByKey(add).map(lambda x: ((reverse_name_mapping[x[0][0]], reverse_name_mapping[x[0][1]]), x[1])).sortByKey()

    for (s, t), v in results.collect():
        print('({}, {}), {}'.format(s, t, v))
    pass


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Invalid arguments!")
        exit(-1)
    worker(input_file=sys.argv[1])
