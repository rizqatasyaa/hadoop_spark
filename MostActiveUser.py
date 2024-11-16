from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as func

# Initialize the Spark session
spark = SparkSession.builder.appName("MostActiveUsers").getOrCreate()

# Define the schema for the ratings.data file
myschema = StructType([\
                       StructField("userID", IntegerType(), True),
                       StructField("movieID", IntegerType(), True),
                       StructField("rating", IntegerType(), True),
                       StructField("ratingTime", IntegerType(), True)
                       ])

# Load the data from the ratings.data file
ratings = spark.read.format("csv")\
    .schema(myschema)\
    .option("path", "hdfs:///user/maria_dev/spark/ratings.data")\
    .load()

# Print schema to check
ratings.printSchema()

# Group by userID and count the number of ratings per user
user_activity = ratings.groupBy("userID")\
    .agg(func.count("rating").alias("total_ratings"))

# Filter and sort users by the number of ratings in descending order
sorted_user_activity = user_activity.orderBy("total_ratings", ascending=False)

# Show the top 10 most active users
top_10_users = sorted_user_activity.limit(10)

# Display the result
top_10_users.show()

# Optionally, you can save the output in a specific format like JSON
top_10_users.write\
    .format("json").mode("overwrite")\
    .option("path", "hdfs:///user/maria_dev/spark/job_output/most_active_users/")\
    .save()
