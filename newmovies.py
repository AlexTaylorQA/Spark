from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf

conf1 = SparkConf()
sc = SparkContext(conf = conf1)
sql = SQLContext(sc)



schema = StructType(
		[
			StructField('id', LongType(), True),
			StructField('genre', StringType(), True),
		])

DF1 = sql.createDataFrame(therecords,schema)

DF1.show()