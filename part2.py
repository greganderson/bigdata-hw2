import sys
from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


### CODE STARTS ###

if len(sys.argv) != 3:
	print 'Invalid arguments.'
	print 'Usage: spark-submit part2.py <graph_file> <number_of_nodes>'
	exit(1)

a_rows = int(sys.argv[2])

file1 = sys.argv[1]
f = sc.textFile(file1)

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))


### COMPUTE A^2

# Map the values to their correct positions in the result matrix
r1 = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(a_rows)])
r2 = a.flatMap(lambda (j, k, value): [((i, k), value) for i in range(a_rows)])

# Pair the elements
z = zip(r1.sortByKey().collect(), r2.sortByKey().collect())
zc = sc.parallelize(z)

# Compute dot product
a = zc.map(lambda (x, y): (x[0], x[1]*y[1]))
answer = a.reduceByKey(lambda x, y: x+y)

### COMPUTE +A

# Pair the elements
z = zip(answer.sortByKey().collect(), r1.sortByKey().collect())
zc = sc.parallelize(z)

# Compute dot product
answer = a.reduceByKey(lambda x, y: x+y)


r = answer.sortByKey().collect()
with open('result.txt', 'w') as fl:
	fl.write(str(r))
