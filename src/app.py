"""
An example stored procedure. __main__ provides an entrypoint for local development
and testing.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import col, DataFrame
from snowflake.snowpark.types import IntegerType, VariantType, Variant
from src import queries, util
from src.util import util
import sys

# Corresponds to configs added to the app.toml
CONFIG_NAME = 'qa6'

# Enable grouped execution
CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = True

# Number of rows in table
N_TABLE_ROWS = 50_000_000
ORIGINAL_TABLE_NAME = f'original_table_{N_TABLE_ROWS}'

# Other parameters that can be controlled
IS_LOCAL = True 
FDN_FILE_SIZE_PARAMETER = None
FAN_IN_PARAMETER = 16
DOP_PARAMETER =  8
RSO_MEMORY_LIMIT_PARAMETER = None
MEMORY_SOFT_LIMIT_PERCENT = None
BATCH_SIZE_MULTIPLIER_PARAMETER = 4
CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT = None
BATCHWISE2_MINIMUM_BATCHSIZE = None
COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = "1,1,1,1,1,1"
USE_ARM_WAREHOUSE = not IS_LOCAL


def main_setup(session, original_table_name, num_rows, use_arm_warehouse):
    pi_dataframe, pi_query_id = queries.setup(session, use_arm_warehouse)
    print(pi_dataframe)
    print(pi_query_id)
    queries.create_original_table(session, FDN_FILE_SIZE_PARAMETER, original_table_name, num_rows)

def main_cluster_manual(session):
    queries.reset_session_system_parameters(session)
    queries.create_test_table(session, "test_table", ORIGINAL_TABLE_NAME)
    queries.reset_table_parameters(session, "test_table")
    queries.remove_defragmentation(session, "test_table")
    queries.setup_manual_clustering(session, "test_table", queries.ClusteringParameters(
        FAN_IN_PARAMETER,
        RSO_MEMORY_LIMIT_PARAMETER,
        MEMORY_SOFT_LIMIT_PERCENT,
        DOP_PARAMETER,
        BATCH_SIZE_MULTIPLIER_PARAMETER,
        CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
        BATCHWISE2_MINIMUM_BATCHSIZE,
        CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
        COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL
    ))

    # Run manual clustering for each of the batches.
    _, query_id = queries.run_manual_clustering(session, "test_table", CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED, 0, 1)
    print(util.query_id_to_snovi_url(query_id))

def main_cluster_auto(session):
    queries.reset_session_system_parameters(session)
    queries.create_test_table(session, "test_table", ORIGINAL_TABLE_NAME)
    queries.reset_table_parameters(session, "test_table")
    queries.remove_defragmentation(session, "test_table")

    queries.setup_auto_clustering(session, "test_table", queries.ClusteringParameters(
        FAN_IN_PARAMETER,
        RSO_MEMORY_LIMIT_PARAMETER,
        MEMORY_SOFT_LIMIT_PERCENT,
        DOP_PARAMETER,
        BATCH_SIZE_MULTIPLIER_PARAMETER,
        CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
        BATCHWISE2_MINIMUM_BATCHSIZE,
        CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
        COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL
    ))

    _, query_id = queries.run_auto_clustering(session, "test_table")
    print(util.query_id_to_snovi_url(query_id))


if __name__ == "__main__":
    from src.util.local import get_env_var_config, get_dev_config

    print("Creating session...")
    print(get_dev_config(config_name=CONFIG_NAME))
    session = Session.builder.configs(get_dev_config(config_name=CONFIG_NAME)).create()

    print("Adding import...")
    session.add_import(util.__file__, 'src.util.util')
    session.add_import(queries.__file__, 'src.queries')

    print("Running queries...")
    command = sys.argv[1]
    if command == "setup":
        main_setup(session, ORIGINAL_TABLE_NAME, N_TABLE_ROWS, USE_ARM_WAREHOUSE)
    elif command == "cluster_manual":
        main_cluster_manual(session)
    elif command == "cluster_auto":
        main_cluster_auto(session)
    else:
        print(f"Invalid command {command}")

    print("Queries complete")
