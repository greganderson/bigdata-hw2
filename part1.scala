from pyspark import SparkContext, SparkConf
import org.apache.spark.mllib.linalg.Vector;

/*********** CONFIGURATION ***********/

val conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("reduce")
conf.set("spark.executor.memory", "4g")

val sc = SparkContext(conf=conf)

blocksize = 1


/*********** CODE STARTS ***********/

val f = sc.textFile("matrices/a_100x200.txt")
val v = sc.textFile("matrices/xx_200.txt")

// Split then convert strings to numbers
val mat_raw = f.map(s => s.split(" "))
							 .map(row => (row[0].asInstanceOf[Int] / blocksize, [row[2].asInstaceOf[Double]]))
							 .reduceByKey(a,b => a+b)
							 .map((k,v) => v)

val vec_raw = v.map(x => [x.asInstanceOf[Double]])
val vec = matrix(vec_raw.collect())
val result = matrix(mat_raw.collect()) * vec

val mat = empty([100, 200], dtype=float)
val data = mat_raw.collect()
for v in data:
	mat[v[0]][v[1]] = v[2]

// Put the matrices into rdd's
val matrix = sc.parallelize(mat.tolist())
val vector = sc.parallelize(vec.tolist())
