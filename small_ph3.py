from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Spark_Assignment_phase1")
sc = SparkContext(conf = conf)
import sys
def parseLine1(line):
    fields = line.split(',') 
    return (int(fields[0]) ,int(fields[1]))
lines = sc.textFile("file:///D:/Bigdata/Assignments2/phase3/SmallDataSet.csv")
rdd1 = lines.map(parseLine1)
rdd=rdd1.map(lambda x: (x[0],[x[1]]))
flip_rdd=rdd1.map(lambda x: (x[1], [x[0]]))
#swaplist=rdd1.map(lambda x:(x[1],x[0]))
merge=flip_rdd.union(rdd)
results = merge.reduceByKey(lambda x, y: (x+y))
sorted_result=results.sortByKey()
bc_dict=sc.broadcast(sorted_result.collectAsMap())  #broad casted the data for lookup purpose
small_graph=sorted_result.collect()
output=sc.parallelize(small_graph)
print("Phase one output for the small graph: " +'\n' +str(small_graph))
res=output.map(lambda x: [(x[0],x[1][j]) for j in range(len(x[1]))])
#ph_2=res.map(lambda x : [(x[i][0],[x[i][1],bc_dict.value[x[i][1]]]) for i in range(len(x))]).collect()
ph_2=res.map(lambda x : (x[0][0],[(x[i][1],bc_dict.value[x[i][1]]) for i in range(len(x))])).collect() #called the dict for looking up the values.
print('\n' +"Phase two output for the small graph :"+ "\n"+str(ph_2))
phase2_small_op= sc.parallelize(ph_2)
#phase2_small_op.saveAsTextFile("file:///D:/Bigdata/Assignments2/Phase2/ph2_small_graph_output.txt")
ph_3=res.map(lambda x : (x[0][0],[(x[i][1],bc_dict.value[x[i][1]]) for i in range(len(x))]))
def triangles (m):
    m=list(m)
    result=[]
    for i in range(0,len(m[1])):
        for j in range(0,len(m[1][i][1])):
            for k in range(len(m[1])):
                if m[1][i][1][j]==m[1][k][0]:
                    result.append([m[0],m[1][i][0],m[1][i][1][j]])
    unique_combinations = list(set(tuple(sorted(node)) for node in result)) 
    return m[0],unique_combinations
unique_combinations_small=ph_3.map(triangles).collect()
print("Phase three out for the tiny graph"+'\n')
phase3_tiny_op= sc.parallelize(unique_combinations_small).saveAsTextFile("file:///D:/Bigdata/Assignments2/phase3/ph3_small_graph_output.txt")
print(unique_combinations_small)