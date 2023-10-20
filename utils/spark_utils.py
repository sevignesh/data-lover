"""
Description: Utility methods for Spark
__author__= "Esakkivignesh"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def create_spark_session(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.14.0") \
        .getOrCreate()
    return spark


def transform_df(df):
    df = df.withColumn("Normalized Salary", col("Salary") * 1.1)
    df = df.withColumn(
        "Salary Outlier",
        when((col("Normalized Salary") < 40000) | (col("Normalized Salary") > 150000), "Outlier")
        .otherwise("Not Outlier"),
    )
    return df

