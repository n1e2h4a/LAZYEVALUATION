from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import time

os.environ["PYSPARK_PYTHON"] = '/usr/bin/python3'
sc = SparkContext()
sparkSession = SparkSession(sc)
# create a RDD of the text file with Number of Partitions = 4
my_text_file = sc.textFile('data.txt', minPartitions=4)
# RDD Object
print(my_text_file)
# time.sleep(20)
print(os.linesep)

# convert to lower case
print("Convert to lower case:")
my_text_file = my_text_file.map(lambda x: x.lower())
# Updated RDD Object
print(my_text_file)
# Get the RDD Lineage
print(my_text_file.toDebugString())
# time.sleep(20)
print(os.linesep)

# slice the words
print("Slicing first 2 chars of each words:")
my_text_file = my_text_file.map(lambda x: x[:2])
# RDD Object
print(my_text_file)
# Get the RDD Lineage
print(my_text_file.toDebugString())
# time.sleep(20)
print(os.linesep)

# Get the first element after all the transformations
print("Getting the first element after all the transformations:")
print(my_text_file.first())
print(os.linesep)

print("Counting unique words:")
print(my_text_file.countApproxDistinct())
# time.sleep(30)
