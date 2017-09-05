from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd = sc.textFile("users.txt")

def records(x):
	info = x.split(",")

	if info[2] != 'Gender':
		
		if info[2] == 'M':
			info[2] = 'Male'
		else:
			info[2] = 'Female'
	
	return info


Rdd1 = Rdd.map(records)


#data = Rdd.collect()
data = Rdd1.collect()

for record in data:
	print(record)
	
	

