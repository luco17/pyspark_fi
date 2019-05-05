#Connect to cluster by creating an instance of SparkContext.
#SparkConf() can hold all of these attributes

#call sparkcontext sc and check it's running
print(sc)
print(sc.version)

#Spark's data structure is the RDD. Splits data across multiple nodes
#Spark DataFrame is an abstracton built on top of RDD, behaves like SQL table
#To work with SparkDF you need to create a SparkSession object, in this case called my_spark

from pyspark.sql import SparkSession

#Returns existing session or creates new one if none available
my_spark = sparkSession.builder.getOrCreate()

#View all tables within the object, .catalog lists all data in the cluster
my_spark.catalog.listTables()

#Passing SQL-like queries using the .sql method
df = my_spark.sql('FROM x SELECT * LIMIT 10')
df.show()

#You can take a SparkDF and turn it into a PdDf
df1 = my_spark.sql('SELECT x, y, COUNT(*) as N FROM tab GROUP BY x, y')
pd_df1 = df1.toPandas()

#Creating SparkDF from pandas DF
#This stores locally, can use SparkDF methods, but not access data in other contexts
import pandas as pd, numpy as np
taw1 = pd.DataFrame(np.random.random(10))

staw1 = my_spark.createDataFrame(taw1)
#Adding temp Spark table to Catalog and checking it's added
staw1.creatOrReplaceTempView('temp')
my_spark.catalog.listTables()

#Reading files straight into spark
file_path = '/usr/path/to/file.csv'
df2 = my_spark.read.csv(file_path, header = True)
df2.show()

#Spark DF is immutable, so to change data, you must reassign each time
#create a df based on data in the .Catalog
df = my_spark.table('datax')

#showing first 5 columns
df.show(5)

#Add new column, note reassign due to immutability
df = df.withColumn('duration_hrs', df.time_mins / 60)

#Filtering with SQL strings, can take 2 different approaches
fil1 = df.filter('distance > 1000')
fil2 = df.filter(df.distance > 1000)

#Selecting, note again, 2 approaches
sel1 = df.select('x', 'y', 'z')
sel2 = df.select(df.x, df.y, df.z)

#Running two pre-created filters on the selected columns
filA = df.origin == 'X'
filB = df.origin == 'Y'
sel3 = sel2.filter(filA).filter(filB)

#Aliasing and creating new tables, 2 approaches
avg_speed = (df.distance / (df.duration / 60)).alias('avg_speed')
df1 = df.select('x', 'y', 'z', avg_speed)
df2 = df.selectExpr('x', 'y', 'z', 'distance / (duration/60) as avg_speed')

#Aggregating
df.filter(df.x == 'JAM').groupBy().min('distance').show()
df.filter(df.x == 'JAM').groupBy().max('distance').show()
df.filter(df.x == 'JAM').filter(df.x == 'ROCK').groupBy().avg('duration').show()
df.withColumn('newCol', df.x / 60).groupBy().sum('duration').show()

#Group Aggregating
grp1 = df.groupBy('criteria')
grp1.count().show()
grp2 = df.groupBy('crit2')
grp2.avg('col1').show()

#Agging with the .sql.functions submodule
import pyspark.sql.functions as F
grp1 = df.groupBy('x', 'y')
grp1.avg('col1').show()
grp1.agg(F.stddev('col1')).show()

#Joins
df1 = df1.withColumnRenamed('old_name', 'new_name')
df_joined = df.join(df2, on = 'new_name', how = 'leftouter')

#ML Pipelining, Spark can only handle numerics
#Casting to int
model_df = model_df.withColumn('col1', model_df.col1.cast('integer'))
#Creating new column
model_df = model_df.withColumn('age', model_df.year - model_df.gadget_age)
#Converting booleans
model_df = model_df.withColumn('is_late', model_df.time > 0)
model_df = model_df.withColumn('label', model_df.is_late.cast('integer'))
#Removing missing values
model_df = modelf_df.filter('x IS NOT NULL AND y IS NOT NULL')

#To handle strings you need to create "OneHotVectors", which represent strings as arrays with all zero elements bar one
#Process: 1) Create a String Indexer, this creates Estimators that take DFs with strings and maps each string to a number
# 2) Estimator returns a Transformer, which takes DF, attaches mapping as metadata
# 3) returns new df with numeric column corresponding to string column
# 4) encode numeric as a one-hot vector using OneHotEncoder.
df_indx = StringIndexer(inputCol = 'col_x', outputCol = 'col_x_index')
df_encd = OneHotEncoder(inputCol = 'col_x_index', outputCol = 'col_x_fact')

#To run the model, all data must be in a single column VectorAssembler does this
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

vec_assembler = VectorAssembler(inputCols = ['col1', 'col2', 'col3'], outputCol = 'features')
tmp_pipe = Pipeline(stages = [df_indx, df_encd, vec_assembler])

#Fitting the data
piped_data = tmp_pipe.fit(model_df).transform(model_df)

#Train/test Split
training, test = piped_data.randomSplit([.6, .4])

#Logreg
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression()

#Evaluating performance, in this case the area under curve (AUC)
import pyspark.ml.evaluation as evals
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderROC')

#Creating a grid of tuning parameters
import pyspark.ml.tuning as tune
grid = tune.ParamGridBuilder()
