from pyspark import SparkContext, SparkConf
import numpy as np

### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

blocksize = 5


#### CODE STARTS ###

m = sc.textFile('matrices/a_100x200.txt')
v = sc.textFile('small.txt')

# Split then convert strings to numbers
mat_raw = f.map(lambda s: s.split(' ')).map(lambda row: [int(row[0]), int(row[1]), float(row[2])])

vec_raw = v.map(lambda x: float(x))

mat = np.empty([100, 200], dtype=float)
data = mat.collect()
for v in data:
	mat[v[0]][v[1]] = v[2]


matrix = sc.parallelize(mat.tolist())
vector = sc.parallelize(np.matrix(vec_raw.collect()).tolist())


with open('test.txt', 'w') as fl:
	fl.write(str(result))
