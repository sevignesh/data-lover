"""
Description: Utility methods to fetch data from data sources
__author__= "Esakkivignesh"
"""

import yaml


def read_data(spark, excel_file, sheet_name):
    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sheetName", sheet_name) \
        .load(excel_file)
    return df


def get_expectations(exp_file):
    with open(exp_file, 'r') as yaml_file:
        expectations_config = yaml.safe_load(yaml_file)
    return expectations_config.get("expectations", [])
