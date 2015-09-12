from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


#### CODE STARTS ###

f = sc.textFile('matrices/a_100x200.txt')
g = sc.textFile('matrices/b_200x100.txt')


# a = 4x3
# b = 3x4
# c = 4x4

a_rows = 100
b_cols = 100

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

# Map the values to their correct positions in the result matrix
result_a = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
result_b = b.flatMap(lambda (i, j, value): [((i, k), value) for k in range(a_rows)])

# Do we union these lists then reduce by key?
a_b = result_a.union(result_b)

answer = a_b.reduceByKey(lambda x,y: x*y)

r = answer.collect()
with open('result.txt', 'w') as fl:
	fl.write(str(r))
#answer.saveAsTextFile('result')
