from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


#### CODE STARTS ###

file1 = 'matrices/2x2a.txt'
#2x2a.txt
#0 0 1
#0 1 2
#1 0 3
#1 1 4

file2 = 'matrices/2x2b.txt'
#2x2b.txt
#0 0 5
#0 1 6
#1 0 7
#1 1 8

#file1 = 'matrices/10x10.txt'
#file2 = 'matrices/5x10.txt'
#file1 = 'matrices/1x2.txt'
#file2 = 'matrices/2x3.txt'
#file1 = 'matrices/a_100x200.txt'
#file2 = 'matrices/b_200x100.txt'

f = sc.textFile(file1)
g = sc.textFile(file2)

#The reason why it would throw an error for rdds not 
# being the same size is because I kept forgetting to
# change these variables to the correct size.
#a_rows = 100
#b_cols = 100

a_rows = 2
b_cols = 2

# Split then convert strings to numbers
a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

#am =a.map(lambda c: (c, 1))

#a_rows = a.filter(lambda x: x[1] == 0).map(lambda c: 1).reduce(lambda a, b: (1+b))

#a_rows = am.reduce(lambda a, b: ((a+b) if a[0][0] == 0 else b))
#a_rows = am.reduceByKey(lambda a, b: (a+b))


# Map the values to their correct positions in the result matrix
r1 = a.flatMap(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
r2 = b.flatMap(lambda (j, k, value): [((i, k), value) for i in range(a_rows)])

#Change: Sort them before we actually zip them
r1 = r1.sortByKey()
r2 = r2.sortByKey()

#Change: zip r1 & r2 as rdds
#zc = r1.join(r2)
#zc = r1.zip(r2)

# Pair the elements
z = zip(r1.collect(), r2.collect())
zc = sc.parallelize(z)


# Compute dot product
a = zc.map(lambda (x, y): (x[0], x[1]*y[1]))
answer = a.reduceByKey(lambda x, y: x+y)

r = answer.sortByKey().collect()


with open('result.txt', 'w') as fl:
	fl.write(str(r))
