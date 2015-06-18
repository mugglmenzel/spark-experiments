"""query1.py"""
from pyspark import SparkContext
import time

current_milli_time = lambda: int(round(time.time() * 1000))
start = current_milli_time()
file = open("/home/d062844/spark-1.0.2-bin-hadoop2/code/logTest.txt", "a")
file.write("\n\n")
file.write("====================================\n")
file.write("== RUNNING QUERY I                ==\n")
file.write("====================================\n\n")

sc = SparkContext("spark://10.97.31.179:7077", "Query I")

file.write("Spark Context: " + str(current_milli_time()-start) + "\n")
start = current_milli_time()

data1 = sc.textFile("/srtec/kjan2009.csv")
data2 = sc.textFile("/srtec/kjun2009.csv")
data3 = sc.textFile("/srtec/kmar2009.csv")
data4 = sc.textFile("/srtec/kmay2009.csv")

file.write("Loading Files: " + str(current_milli_time()-start) + "\n")
start = current_milli_time()

multiply = 1
    
for x in range(1, 11):
    count1 = data1.count() * multiply
    count2 = data2.count() * multiply
    count3 = data3.count() * multiply
    count4 = data4.count() * multiply

    file.write("Run " + str(x) + ": " + str(current_milli_time()-start))
    start = current_milli_time()
    file.write(" (JAN: %i, JUN: %i, MAR: %i, MAY: %i) \n" % (count1, count2, count3, count4))

file.close()
