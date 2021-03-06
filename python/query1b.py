"""query1b.py"""
from pyspark import SparkContext

import time

current_milli_time = lambda: int(round(time.time() * 1000))
start = current_milli_time()
file = open("/home/d062844/spark-1.0.2-bin-hadoop2/code/log.txt", "a")
file.write("\n\n")
file.write(" ====================================\n")
file.write(" =  RUNNING QUERY Ib (ma            =\n")
file.write(" ====================================\n\n")

sc = SparkContext("spark://10.97.31.179:7077", "Query V")

file.write("Spark Context: " + str(current_milli_time()-start) + "\n\n")
start = current_milli_time()

data1 = sc.textFile("/srtec/kjan2009.csv").map(lambda line: (1, 1))
data2 = sc.textFile("/srtec/kjun2009.csv").map(lambda line: (2, 1))
data3 = sc.textFile("/srtec/kmar2009.csv").map(lambda line: (3, 1))
data4 = sc.textFile("/srtec/kmay2009.csv").map(lambda line: (4, 1))

union1 = data1.union(data2)
union2 = data3.union(data4)
union = union1.union(union2).cache()

file.write("Loading Files: " + str(current_milli_time()-start) + "\n")
start = current_milli_time()

addup = 0
run = 10

for x in range(0, run):
    reduced = union.countByKey()
    
    #result = reduced.collect()
    #print reduced
    
    file.write("Run " + str(x) + ": " + str(current_milli_time()-start) + "\n")
    if(x > 0):
        addup = addup + current_milli_time()-start
    start = current_milli_time()

file.write("\nAverage: " + str(round(addup/(run-1))))

file.close()
