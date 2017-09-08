from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf1 = SparkConf()
sc = SparkContext(conf = conf1)
sqlC = SQLContext(sc)

def splitRecord(rec):
	record = rec.split(",")
	return record

# --
	
Rdd1 = sc.textFile("storefiles/category.txt")
records1 = Rdd1.map(lambda X: splitRecord(X))

schema1 = StructType(
		[
			StructField("CID", StringType()),
			StructField("TITLE", StringType())
		]
	)

category = sqlC.createDataFrame(records1, schema1)
category.registerTempTable("TableCategory")

# --

Rdd2 = sc.textFile("storefiles/subcategory.txt")
records2 = Rdd2.map(lambda X: splitRecord(X))

schema2 = StructType(
		[
			StructField("CID", StringType()),
			StructField("SID", StringType()),
			StructField("TITLE", StringType())
		]
	)

subcategory = sqlC.createDataFrame(records2, schema2)
subcategory.registerTempTable("TableSubcategory")

# --

Rdd3 = sc.textFile("storefiles/products.txt")
records3 = Rdd3.map(lambda X: splitRecord(X))

schema3 = StructType(
		[
			StructField("PID", StringType()),
			StructField("NAME", StringType()),
			StructField("SID", StringType())
		]
	)

products = sqlC.createDataFrame(records3, schema3)
products.registerTempTable("TableProducts")

# --

Rdd4 = sc.textFile("storefiles/sales.txt")
records4 = Rdd4.map(lambda X: splitRecord(X))

schema4 = StructType(
		[
			StructField("SALESID", StringType()),
			StructField("PID", StringType()),
			StructField("QTY", StringType()),
			StructField("PRICE", StringType())
		]
	)

sales = sqlC.createDataFrame(records4, schema4)
sales.registerTempTable("TableSales")

category.show()
subcategory.show()
products.show()
sales.show()

X1 = sqlC.sql("select SALESID as Category, CAST(QTY * 100 AS INT) as Quantitydddd from TableSales")
X1.show()

X2 = sqlC.sql("select TITLE from TableSubcategory where CID = (select max(CID) from TableCategory)")