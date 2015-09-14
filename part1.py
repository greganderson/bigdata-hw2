import sys
from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


#### CODE STARTS ###

if len(sys.argv) != 5:
	print 'Invalid arguments.'
	print 'Usage: spark-submit part1.py <matrix_file_1> <matrix_1_dimensions> <matrix_file_2> <matrix_2_dimensions>'
	exit(1)

file1 = sys.argv[1]
file2 = sys.argv[3]

f = sc.textFile(file1)
g = sc.textFile(file2)

a_rows = int(sys.argv[2][:sys.argv[2].find('x')])
b_cols = int(sys.argv[4][:sys.argv[4].find('x')])

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

# Map the values to their correct positions in the result matrix
r1 = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
r2 = b.flatMap(lambda (j, k, value): [((i, k), value) for i in range(a_rows)])

# Pair the elements
z = zip(r1.sortByKey().collect(), r2.sortByKey().collect())
zc = sc.parallelize(z)

# Compute dot product
a = zc.map(lambda (x, y): (x[0], x[1]*y[1]))
answer = a.reduceByKey(lambda x, y: x+y)


#r = answer.sortByKey().collect()
#with open('result.txt', 'w') as fl:
	#fl.write(str(r))
answer.saveAsTextFile('result')
















m = #
n = #
p = #
blocksize = 1

a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]) / blocksize, ((int(row[0]), int(row[1])), float(row[2]))))
g = a.groupByKey()


def dot(a, b):
	total = 0
	for i in range(len(a)):
		total += a[i] * b[i]
	return total


def dot_group(a, b):
	mat = []
	for i in range(len(a)):
		mat.append([])
		for j in range(len(b)):
			mat[i].append(dot(a[i], b[j]))
	return mat


	for i in range(len(a)):

		# Create the vectors
		r1 = []
		for j in range(m):
			r1.append(a[j][1])

		for j in range(p):
			r2 = []
			for j in range(p):
				r2.append(b[1])
		
			mat[i].append(dot(r1, r2))

b = g.map(dot_group)
		







def func(a, b):
	for i in range(len(a)):
		for j in range(len(b)):
			# 





