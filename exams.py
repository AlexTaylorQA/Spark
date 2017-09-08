from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd = sc.textFile("exams.txt")

def theValues(x):
	newRecords = x.split(",")
	return (newRecords[0], newRecords[2])

def add(x,y):
	return int(x) + int(y)

Rdd1 = Rdd.map(theValues)
data1 = Rdd1.reduceByKey(add)
data2 = data1.collect()

print(data2)