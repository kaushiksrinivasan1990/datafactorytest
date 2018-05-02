
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkContext
import subprocess

sc = SparkContext()
spark = SparkSession(sc)

hvacText = sc.textFile("/test/sample.csv")

Entry = Row('Animal', 'numof', 'weight', 'Type')

hvacParts = hvacText.map(lambda s: s.split(','))

hvac = hvacParts.map(lambda p: (str(p[0]), int(p[1]), float(p[2]), str(p[3])))

schemasample = spark.createDataFrame(hvac, Entry)

schemasample.registerTempTable('hvactemptable')

sample2 = spark.sql("select SUM(numof+weight), Animal from hvactemptable group by Animal ")

subprocess.call(["hadoop", "fs", "-rm", "-r", "/test/output/"])
sample2.coalesce(1).write.csv('/test/output/')

