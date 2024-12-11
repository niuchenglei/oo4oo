# Imports MLeap serialization functionality for PySpark
import mleap.pyspark
from mleap.pyspark.spark_support import SimpleSparkSerializer

# Import standard PySpark Transformers and packages
from pyspark.ml.feature import VectorAssembler, StandardScaler, OneHotEncoder, StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import Row

# Create a test data frame
l = [('Alice', 1), ('Bob', 2)]
rdd = sc.parallelize(l)
Person = Row('name', 'age')
person = rdd.map(lambda r: Person(*r))
df2 = spark.createDataFrame(person)
df2.collect()

# Build a very simple pipeline using two transformers
string_indexer = StringIndexer(inputCol='name', outputCol='name_string_index')

feature_assembler = VectorAssembler(inputCols=[string_indexer.getOutputCol()], outputCol="features")

feature_pipeline = [string_indexer, feature_assembler]

featurePipeline = Pipeline(stages=feature_pipeline)

fittedPipeline = featurePipeline.fit(df2)

