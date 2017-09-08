from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf1 = SparkConf()
sc = SparkContext(conf = conf1)
sql = SQLContext(sc)

def splitRecord(rec):
	record = rec.split(",")
	return (int(record[0]), record[1], record[2], record[3], int(record[4]))

def ddouble(A):
	return A*A

abc = udf(ddouble, IntegerType())
	
Rdd1 = sc.textFile("users.txt")
therecords = Rdd1.map(splitRecord)

schema = StructType(
		[
			StructField('id', LongType(), True),
			StructField('name', StringType(), True),
			StructField('gender', StringType(), True),
			StructField('group', StringType(), True),
			StructField('marks', LongType(), True)
		])

#record1 = therecords.map(lambda X: X.split(","))
#header=record1.first()
#records=record1.filter(lambda X: X<>header)

DF1 = sql.createDataFrame(therecords,schema)

print("--- DF1.printSchema")
DF1.printSchema()

print("--- DF1.show()")
DF1.show()
print(DF1.count())

DF1.select("id", "marks").show()

DF1.select(DF1.id).show()

DF1.select(DF1.id).filter(DF1.id > 103).show()

DF1.filter(DF1.id > 103).select(DF1.id.alias("USERID"), DF1.marks.alias("MARKS")).show()

DF1.sort(DF1.marks.asc()).show()

DF1.groupby("group").count().show()
DF1.groupby("group").sum("marks").show()
DF1.groupby("group").sum("id", "marks").show()

DF1.select("id", "name", abc("id").alias("QA")).show()