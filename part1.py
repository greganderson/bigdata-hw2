from pyspark import SparkContext, SparkConf
from numpy import matrix, array, empty

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

blocksize = 1


#### CODE STARTS ###

f = sc.textFile('matrices/a_100x200.txt')
v = sc.textFile('matrices/xx_200.txt')

# Split then convert strings to numbers
mat_raw = f.map(lambda s: s.split(' ')) \
					 .map(lambda row: (int(row[0]) / blocksize, [float(row[2])])) \
					 .reduceByKey(lambda a,b: a+b) \
					 .map(lambda (k,v): v)

vec_raw = v.map(lambda x: [float(x)])
vec = matrix(vec_raw.collect())
result = matrix(mat_raw.collect()) * vec

mat = empty([100, 200], dtype=float)
data = mat_raw.collect()
for v in data:
	mat[v[0]][v[1]] = v[2]

# Put the matrices into rdd's
matrix = sc.parallelize(mat.tolist())
vector = sc.parallelize(vec.tolist())
#vector = sc.parallelize(matrix(vec_raw.collect()).tolist())


with open('test.txt', 'w') as fl:
	fl.write(str(result))












# a = 4x3
# b = 3x4
# c = 4x4

a_rows = 4
b_cols = 4

a = f.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))
b = g.map(lambda s: s.split(' ')).map(lambda row: (int(row[0]), int(row[1]), float(row[2])))

result_a = a.map(lambda (i, j, value): [((i, k), value) for k in range(b_cols)])
result_b = b.map(lambda (i, j, value): [((i, k), value) for k in range(a_rows)])

answer = result_a.reduceByKey(lambda x,y: x*y, result_b)
