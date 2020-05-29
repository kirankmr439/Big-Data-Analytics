from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Spark_Assignment_phase1")
sc = SparkContext(conf = conf)
import sys
def parseLine1(line):
    fields = line.split(',') 
    return (int(fields[0]) ,int(fields[1]))
    
    
lines = sc.textFile("file:///D:/Bigdata/Assignments2/SmallDataSet.csv")
rdd1 = lines.map(parseLine1)
rdd=rdd1.map(lambda x: (x[0],[x[1]]))
flip_rdd=rdd1.map(lambda x: (x[1], [x[0]]))
#swaplist=rdd1.map(lambda x:(x[1],x[0]))
merge=flip_rdd.union(rdd)
results = merge.reduceByKey(lambda x, y: (x+y))
sorted_result=results.sortByKey()
small_graph=sorted_result.collect()
for i in range(len(small_graph)):
    print("Node "+ str(small_graph[i][0])  +": connected to the following list of nodes "+ str(small_graph[i][1]) + '\n')  

output=sc.parallelize(small_graph)
output.saveAsTextFile("file:///D:/Bigdata/Assignments2/small_graph_output.txt")
print(small_graph)
