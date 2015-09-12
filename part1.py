from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


#### CODE STARTS ###

#file1 = '1x2.txt'
#file2 = '2x3.txt'
file1 = 'matrices/a_100x200.txt'
file2 = 'matrices/b_200x100.txt'

f = sc.textFile(file1)
g = sc.textFile(file2)

a_rows = 100
b_cols = 100

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

# Map the values to their correct positions in the result matrix
r1 = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
r2 = b.flatMap(lambda (j, k, value): [((i, k), value) for i in range(a_rows)])

# Pair the elements
z = zip(r1.collect(), r2.collect())
zc = sc.parallelize(z)

# Compute dot product
a = zc.map(lambda (x, y): (x[0], x[1]*y[1]))
answer = a.reduceByKey(lambda x, y: x+y)


r = answer.sortByKey().collect()
with open('result.txt', 'w') as fl:
	fl.write(str(r))
#answer.saveAsTextFile('result')
