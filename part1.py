from pyspark import SparkContext, SparkConf


conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)


def func(x):
	row = x.split(' ')
	row = [int(row[0]), int(row[1]), float(row[2])]
	if row[0] not in mat:
		i = len(mat)
		mat.append([])
	mat[i].append(row[2])


f = sc.textFile('matrices/a_100x200.txt')

mat = []
rdd = f.map(func)

result = rdd.collect()

with open('test.txt', 'w') as fl:
	fl.write(result)
