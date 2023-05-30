from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import os

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

if __name__ == "__main__":
    # Create SparkSession
    spark = SparkSession.builder.master("local[2]").appName("first") \
        .config("spark.es.nodes", "localhost") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Data Structure
    schema = StructType([
        StructField("Protocol", StringType(), True),
        StructField("HTTP status", StringType(), True),
        StructField("URL", StringType(), True),
        StructField("Path", StringType(), True),
        StructField("IP Address", StringType(), True)
    ])

    # Create DataFrame
    StreamDF = spark.read.option("delimiter", " ") \
        .schema(schema) \
        .csv("/home/hdayyoub/Documents/simple/output")

    StreamDF.createOrReplaceTempView("SDF2")
    outDF = spark.sql("SELECT * FROM SDF2")

    # Write DataFrame to Elasticsearch
    outDF.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.port", "9200") \
        .option("es.nodes", "localhost") \
        .mode("append") \
        .save("logs_kibana/doc")

    # Write Stream to Console
    # outDF.writeStream.format("console") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()
