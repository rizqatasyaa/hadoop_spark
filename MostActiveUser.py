from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("MostActiveUsers").getOrCreate()

myschema = StructType([\
                       StructField("userID", IntegerType(), True),
                       StructField("movieID", IntegerType(), True),
                       StructField("rating", IntegerType(), True),
                       StructField("ratingTime", IntegerType(), True)
                       ])

ratings = spark.read.format("csv")\
    .schema(myschema)\
    .option("path", "hdfs:///user/maria_dev/spark/ratings.data")\
    .load()

ratings.printSchema()

user_activity = ratings.groupBy("userID")\
    .agg(func.count("rating").alias("total_ratings"))

sorted_user_activity = user_activity.orderBy("total_ratings", ascending=False)

top_10_users = sorted_user_activity.limit(10)

top_10_users.show()

top_10_users.write\
    .format("json").mode("overwrite")\
    .option("path", "hdfs:///user/maria_dev/spark/job_output/most_active_users/")\
    .save()
