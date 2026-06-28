from pyspark.sql import SparkSession

def get_spark():

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("Tech Challenge")
        .getOrCreate()
    )