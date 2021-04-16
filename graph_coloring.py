from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark.sql import SparkSession
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, functions as sqlfunctions, types
from pyspark.sql import functions as F
from pyspark.sql.functions import lit


spark = SparkSession.builder.appName('graph-coloring-problem').getOrCreate()

vertices = spark.createDataFrame([('1',), 
                                  ('2',),
                                  ('3',),
                                  ('4',)],
                                  ['id'])

edges = spark.createDataFrame([('1', '2'), 
                               ('2', '1'),
                              ('1', '3'),
                              ('3', '1'),
                               ('1', '4'),
                               ('4', '1'),
                               ('3', '4'),
                               ('4', '3')],
                              ['src', 'dst'])

#superstep = 0

vertices = vertices.withColumn('color',lit(-1))
#vertices = vertices.withColumn('maxima',lit('true'))
#vertices = vertices.withColumn('superstep',lit(superstep))

superstep = 1

cached_vertices = AM.getCachedDataFrame(vertices)

#show graph
g = GraphFrame(cached_vertices, edges)
g.vertices.show()
g.edges.show()
g.degrees.show()

def get_max(a, b):
    return a if(a > b) else b
get_max_udf = F.udf(get_max, types.IntegerType())

def get_max_id(id, msg_ids, superstep):
    #print(msg_ids)
    print(superstep)
    if len(msg_ids) == 0: 
        return superstep
    return superstep if(int(max(msg_ids)) < int(id)) else -1
get_max_id_udf = F.udf(get_max_id, types.IntegerType())

max_iterations = 4


for _ in range(max_iterations):
    sub_g = g.filterVertices("color == -1")
    
    if (sub_g.vertices.count() == 1): 
        res = sub_g.vertices.withColumn("newColor", lit(superstep)).drop("color")

        new_vertices = g.vertices.join(res, on="id", how="left_outer") \
                    .withColumnRenamed("color", "oldColor") \
                    .withColumn("color", get_max_udf(F.col("oldColor"), F.col("newColor"))) \
                    .drop("oldColor").drop("newColor")
        
        cached_new_vertices = AM.getCachedDataFrame(new_vertices)
        g = GraphFrame(cached_new_vertices, g.edges)
        g.vertices.show()

        break

    aggregates = sub_g.aggregateMessages(F.collect_set(AM.msg).alias("agg"), sendToDst=AM.src["id"])
    res = aggregates.withColumn("newColor", get_max_id_udf("id", "agg", lit(superstep))).drop("agg")
    
    new_vertices = g.vertices.join(res, on="id", how="left_outer") \
                    .withColumnRenamed("color", "oldColor") \
                    .withColumn("color", get_max_udf(F.col("oldColor"), F.col("newColor"))) \
                    .drop("oldColor").drop("newColor")
    
    superstep = superstep + 1

    cached_new_vertices = AM.getCachedDataFrame(new_vertices)
    g = GraphFrame(cached_new_vertices, g.edges)
    g.vertices.show()



