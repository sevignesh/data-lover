# data-lover

## Overview
Automation Data Validation Suite using Spark and Great Expectations.

## Project Description
This project is designed to implement a robust and scalable automation data validation suite using Spark and Great Expectations for validation of data accuracy, transformation and integrity between source and target systems.

## Project Components
1. PySpark 3.2.1
2. Great Expectations 0.17.12

## System under test
resources/emmployee_data.xlsx - Source data store containing the list of employee information

## Environment Requirements
1. Python v3.10.0 or later
2. Java 1.8.0 or later
3. pip 23.0.0 or later
4. Pycharm or IntelliJ IDE

## Project Structure
1. config: Contains the expectation config file for both source and target to be used as basis for Great Expectations suite.
2. resources: Contains the employee data file which act as the source data store.
3. test: Contains the test implementation using PySpark and Great Expectations for core data validation.
4. utils: Helper methods to be used for validation.

## Execution Instructions
To run the project, follow these steps:
1. Clone the project to your local environment.
2. Create virtualenv & activate the env
   ```
    virtualenv -p python3 venv
    source venv/bin/activate
   ```
3. Install dependencies
   ```
    cd requirements
    pip install -r requirements.txt
   ```
4. Execute the tests
   ```
    python3 test/test_employee_data.py
   ```

## Reports Location
After execution, you can access Great Expectation reports along with it's data docs and expectation config under gx_docs directory.

## CI/CD considerations
1. The framework can be containerized by creating a Dockerfile with all the setup and dependencies involved in it.
2. Dockerfile can then be built and the resultant container image can be pushed to DockerHub/ECR using Github Actions.
3. CI/CD execution can be taken care as part of the same Github Actions workflow itself.
