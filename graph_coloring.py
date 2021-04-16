from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName('graph-coloring-problem').getOrCreate()

vertices = spark.createDataFrame([('1',), 
                                  ('2',),
                                  ('3',),
                                  ('4',), 
                                  ('5',),
                                  ('6',),
                                  ('7',),
                                  ('8',), 
                                  ('9',), 
                                  ('10',)],
                                  ['id'])

edges = spark.createDataFrame([('1', '2'), 
                                ('2', '1'),
                                ('1', '3'),
                                ('3', '1'),
                                ('2', '3'), 
                                ('3', '2'),
                                ('1', '4'),
                                ('4', '1'),
                                ('1', '6'), 
                                ('6', '1'), 
                                ('1', '5'), 
                                ('5', '1'),
                                ('4', '5'),
                                ('5', '4'),
                                ('5', '6'),
                                ('6', '5'), 
                                ('6', '7'), 
                                ('7', '6'), 
                                ('7', '8'), 
                                ('8', '7'), 
                                ('6', '8'), 
                                ('8', '6'), 
                                ('9', '6'), 
                                ('6', '9'), 
                                ('9', '8'), 
                                ('8', '9'), 
                                ('9', '10'), 
                                ('10', '9'), 
                                ('10', '8'), 
                                ('8', '10')],
                                ['src', 'dst'])

#initial color -1
vertices = vertices.withColumn('color',lit(-1))

cached_vertices = AM.getCachedDataFrame(vertices)

#display input graph
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()
g.degrees.show()

#get the greatest between two integers
def get_max(a, b):
    return a if(a > b) else b
get_max_udf = F.udf(get_max, types.IntegerType())


#check if id is greater than its message ids
def check_max(id, msg_ids, superstep):
    tmp = []
    for i in range(len(msg_ids)): 
        tmp.append(int(msg_ids[i]))

    if max(tmp) < int(id): 
        return superstep
    else: 
        return -1
check_max_udf = F.udf(check_max, types.IntegerType())

superstep = 1

while(1):
    sub_g = g.filterVertices("color == -1")
    
    #only one vertice left uncolored
    if (sub_g.vertices.count() == 1): 
        res = sub_g.vertices.withColumn("newColor", lit(superstep)).drop("color")

        #color remaining vertice
        new_vertices = g.vertices.join(res, on="id", how="left_outer") \
                    .withColumnRenamed("color", "oldColor") \
                    .withColumn("color", get_max_udf(F.col("oldColor"), F.col("newColor"))) \
                    .drop("oldColor").drop("newColor")
        
        print("------------ Graph Coloring Solution ------------")
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g = GraphFrame(cached_new_vertices, g.edges)
        g.vertices.show()

        break

    aggregates = sub_g.aggregateMessages(F.collect_set(AM.msg).alias("agg"), sendToDst=AM.src["id"])
    res = aggregates.withColumn("newColor", check_max_udf("id", "agg", lit(superstep))).drop("agg")
    
    #color vertices
    new_vertices = g.vertices.join(res, on="id", how="left_outer") \
                    .withColumnRenamed("color", "oldColor") \
                    .withColumn("color", get_max_udf(F.col("oldColor"), F.col("newColor"))) \
                    .drop("oldColor").drop("newColor")
    
    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g = GraphFrame(cached_new_vertices, g.edges)
    g.vertices.show()

    superstep = superstep + 1
