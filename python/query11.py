"""query1.py"""
from pyspark import SparkContext

print "Running Query III"

sc = SparkContext("local", "Simple App")

# Cache the data.

data0 = sc.textFile("/srtec/klein.csv")
data1 = sc.textFile("/srtec/kjan2009.csv")
data2 = sc.textFile("/srtec/kjun2009.csv")
data3 = sc.textFile("/srtec/kmar2009.csv")
data4 = sc.textFile("/srtec/kmay2009.csv")

union1 = data1.union(data2)
union2 = data3.union(data4)
union = union1.union(union2)

#print "Executing on cached data..."

# Run commands.

reduced = data0.map(lambda line: (line.split()[1], line.split()[1].mktime(datetime.datetime.strptime(s, "%d/%m/%Y %h:%m%s:000").timetuple())))

#mapped = data0.map(lambda line: (1, line.split()[15]))
#reduced = mapped.reduceByKey(lambda a,b: a if(a>b) else b)
#print " [> COUNT <] "
#print reduced.count()
print " [> SNAP <] "
print reduced.take(100)

# Print results.

# spark-submit.cmd ..\code\query5.py
