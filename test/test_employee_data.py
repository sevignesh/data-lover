"""
Description: Validation of Employee data using PySpark and Great Expectations Suite
__author__= "Esakkivignesh"
"""

import sys
from utils.data_utils import read_data
from utils.gx_utils import init_gx_context, gx_batch_process
from utils.spark_utils import create_spark_session, transform_df

if __name__ == "__main__":
    excel_file = f"{sys.path[1]}/resources/employee_data.xlsx"
    source_expectation_config = f"{sys.path[1]}/config/source_gx_config.yaml"
    target_expectation_config = f"{sys.path[1]}/config/target_gx_config.yaml"

    # Initialize Spark context
    spark = create_spark_session(app_name="Employee Data Quality")

    # Initialize Great Expectations Suite
    gx_context = init_gx_context()

    # Read source data from Excel file
    source_employee_df = read_data(spark, excel_file=excel_file, sheet_name="Employees")

    # Run Great Expectations Validation Suite on top of source dataframe
    gx_batch_process(gx_context, source_employee_df, source_expectation_config, data_asset_name="Source Employees")

    # Transform the source dataframe by deriving custom columns
    target_employee_df = transform_df(source_employee_df)

    # Ensure that count remains same after transformation
    assert source_employee_df.count() == target_employee_df.count()

    # Run Great Expectations Validation Suite on top of transformed dataframe
    gx_batch_process(gx_context, target_employee_df, target_expectation_config, data_asset_name="Target Employees")

