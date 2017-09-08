from pyspark import SparkConf, SparkContext

conf1 = SparkConf()
conf1.setMaster("local")
conf1.setAppName("First program")
sc = SparkContext(conf = conf1)

Rdd1 = sc.textFile("exams.txt")
Rdd2 = sc.textFile("personal.txt")

def passfail(x):
	newRecords = x.split(",")
	
	thePercent = ""
	remarks = ""
	if newRecords[2] == 'Marks':
		remarks = "Result"
		thePercent = "Percentage"
	else:
		thePercent = int(newRecords[2])*100/150
		if thePercent >= 60:
			remarks = "Pass"
		else:
			remarks = "Fail"
		
	return ()

Rdd1 = Rdd.map(passfail)

data = Rdd1.collect()



data1 = Rdd1.collect()

print(data2)