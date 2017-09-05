from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
sc = SparkContext(conf = conf1)

List1 = [1,2,3,4,5]

def double(x):
	return x * 2

Rdd = sc.parallelize(List1)


Rdd2 = Rdd.map(double)
Data1 = Rdd2.collect()
Data2= Rdd.collect()

print(Data1)
print(Data2)






