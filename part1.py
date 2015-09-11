from pyspark import SparkContext, SparkConf


### CONFIGURATION ###

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

sc = SparkContext(conf=conf)

blocksize = 5


### CODE STARTS ###

f = sc.textFile('matrices/a_100x200.txt')

# Split then convert strings to numbers
rdd = f.map(lambda s: s.split(' ')).map(lambda row: [int(row[0]), int(row[1]), float(row[2])])


result = rdd.collect()

with open('test.txt', 'w') as fl:
	fl.write(str(result))
