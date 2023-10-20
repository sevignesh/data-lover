"""
Description: Utility methods for Great Expectations Suite
__author__= "Esakkivignesh"
"""

import os
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from great_expectations.data_context.types.resource_identifiers import ExpectationSuiteIdentifier
from utils.data_utils import get_expectations


def init_gx_context():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_context_config = DataContextConfig(
        datasources={
            "spark": DatasourceConfig(
                class_name="Datasource",
            )
        },
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(project_root, "gx_docs", "data_docs")
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder"
                }
            }
        },
        stores={
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory":  os.path.join(project_root, "gx_docs", "expectations")
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory":  os.path.join(project_root, "gx_docs", "validations")
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory":  os.path.join(project_root, "gx_docs", "checkpoints")
                }
            },
            "profiler_store": {
                "class_name": "ProfilerStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory":  os.path.join(project_root, "gx_docs", "profilers")
                }
            }
        },
        validations_store_name="validations_store",
        expectations_store_name="expectations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        checkpoint_store_name="checkpoint_store",
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    }
                ],
            }
        }
    )
    gx_context = gx.get_context(project_config=data_context_config)
    return gx_context


def add_spark_data_source(context, data_asset_name):
    data_source_name = data_asset_name + '_datasource'
    datasource_config = {
        "name": data_source_name,
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
            "force_reuse_spark_context": "true"
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "/Users/cb-it-01-1594/work/ge_spark/",
                "default_regex": {"group_names": [data_asset_name], "pattern": "(.*)"},
            }
        }
    }
    context.add_datasource(**datasource_config)


def generate_expectation_batch(context, data_source_name, data_asset_name, expectation_suite_name, df):
    print("====== generate_expectation_batch =====")
    batch_request = RuntimeBatchRequest(
        datasource_name=data_source_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )
    context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name, overwrite_existing=True
    )
    return batch_request


def add_expectations(context, expectation_suite_name, expectation_config):
    print("====== add_expectations =====")
    suite = context.get_expectation_suite(expectation_suite_name=expectation_suite_name)
    expectations = get_expectations(expectation_config)
    for expectation in expectations:
        expectation_configuration = ExpectationConfiguration(**expectation)
        suite.add_expectation(expectation_configuration=expectation_configuration)
    context.save_expectation_suite(expectation_suite=suite, expectation_suite_name=expectation_suite_name)


def trigger_checkpoint(context, batch_request, expectation_suite_name, checkpoint_name):
    print("====== add_checkpoint =====")
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "run_name_template": "%Y%m%d-%H%M%S-ge-run-template"
    }
    context.add_checkpoint(**checkpoint_config)
    results = context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        validations=[{"batch_request": batch_request, 'expectation_suite_name': expectation_suite_name}]
    )
    print(results)
    return results


def generate_docs(context, expectation_suite_name):
    suite_identifier = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)
    context.build_data_docs(resource_identifiers=[suite_identifier])
    # context.open_data_docs(resource_identifier=suite_identifier)


def gx_batch_process(context, batch_df, expectation_config, data_asset_name):
    data_source_name = data_asset_name + '_datasource'
    expectation_suite_name = data_asset_name + '_expectations'
    checkpoint_name = data_asset_name + '_checkpoint'

    # Add data source
    add_spark_data_source(context, data_asset_name)
    batch_df.printSchema()

    # Generate Batch
    batch_request = generate_expectation_batch(context, data_source_name, data_asset_name, expectation_suite_name,
                                               batch_df)
    # Add Expectation Suite
    add_expectations(context, expectation_suite_name, expectation_config)
    # Trigger Checkpoint
    trigger_checkpoint(context, batch_request, expectation_suite_name, checkpoint_name)
    # Generate Data Docs
    generate_docs(context, expectation_suite_name)
    print('Docs generated for - {}'.format(expectation_suite_name))


