"""query3.py"""
from pyspark import SparkContext
import time

current_milli_time = lambda: int(round(time.time() * 1000))
start = current_milli_time()
file = open("/home/d062844/spark-1.0.2-bin-hadoop2/code/log.txt", "a")
file.write("\n\n")
file.write(" ====================================\n")
file.write(" =  RUNNING QUERY III(Cache Result) =\n")
file.write(" ====================================\n\n")

sc = SparkContext("spark://10.97.31.179:7077", "Query III")

file.write("Spark Context: " + str(current_milli_time()-start) + "\n\n")
start = current_milli_time()

data1 = sc.textFile("/srtec/kjan2009.csv")
data2 = sc.textFile("/srtec/kjun2009.csv")
data3 = sc.textFile("/srtec/kmar2009.csv")
data4 = sc.textFile("/srtec/kmay2009.csv")

union1 = data1.union(data2)
union2 = data3.union(data4)
union = union1.union(union2)
mapped = union.map(lambda line: line.split()[2]).cache()

file.write("Loading Files: " + str(current_milli_time()-start) + "\n")
start = current_milli_time()

addup = 0
run = 10

for x in range(0, run-1):
    distinct = mapped.distinct().cache()
    result = distinct.collect()
    #print distinct.take(100)

    file.write("Run " + str(x) + ": " + str(current_milli_time()-start) + "\n")
    addup = addup + current_milli_time()-start
    start = current_milli_time()

file.write("\nAverage: " + str(round(addup/run)))

file.close()
