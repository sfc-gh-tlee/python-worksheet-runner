"""
An example stored procedure. __main__ provides an entrypoint for local development
and testing.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import col, DataFrame
from snowflake.snowpark.functions import udf, udaf
from snowflake.snowpark.types import IntegerType, VariantType, Variant
from src import functions, typedefs, queries, util
from src.util import util
import sys

FDN_FILE_SIZE_PARAMETER = None
FAN_IN_PARAMETER = 16
RSO_MEMORY_LIMIT_PARAMETER = None # '1000000000' # None # '1000000000' 
MEMORY_SOFT_LIMIT_PERCENT = 90
DOP_PARAMETER =  None # 8
BATCH_SIZE_MULTIPLIER_PARAMETER = 8 # None # 2048 # 1 # 8 # 1
CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT = None
BATCHWISE2_MINIMUM_BATCHSIZE = None # 83886080 * 8 * 8 # None
COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = None # "128,0,0,0,0,0"
CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = False

ORIGINAL_TABLE_NAME = 'original_table_1_000_000_000'

def main_setup(session, original_table_name, num_rows):
    pi_dataframe, pi_query_id = queries.setup(session)
    print(pi_dataframe)
    print(pi_query_id)
    queries.create_original_table(session, FDN_FILE_SIZE_PARAMETER, original_table_name, num_rows)

def main_cluster(session):
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


def main_cluster_2x_dop4(session):
    queries.reset_session_system_parameters(session)

    queries.create_test_table(session, "test_table1", ORIGINAL_TABLE_NAME)
    queries.reset_table_parameters(session, "test_table1")
    queries.remove_defragmentation(session, "test_table1")
    queries.setup_manual_clustering(session, "test_table1", queries.ClusteringParameters(
        FAN_IN_PARAMETER,
        RSO_MEMORY_LIMIT_PARAMETER,
        MEMORY_SOFT_LIMIT_PERCENT,
        8,
        16,
        CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
        BATCHWISE2_MINIMUM_BATCHSIZE,
        CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
    ))

    job1, query_id_1 = queries.run_manual_clustering(session, "test_table1", CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED, 0, 1, use_async=True)
    # job2, query_id_2 = queries.run_manual_clustering(session, "test_table1", CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED, 2, 3, use_async=True)

    print("Query 1:", util.query_id_to_snovi_url(query_id_1))
    # print("Query 2:", util.query_id_to_snovi_url(query_id_2))

    # block so program doesn't terminate.
    job1.result()
    # job2.result()

def main_8_small_ungrouped_batches(session):
    batch_numbers = list(range(8))

    queries.create_test_table(session, f"test_table", ORIGINAL_TABLE_NAME)
    queries.reset_table_parameters(session, f"test_table")
    queries.remove_defragmentation(session, f"test_table")
    queries.setup_manual_clustering(session, f"test_table", queries.ClusteringParameters(
        FAN_IN_PARAMETER,
        RSO_MEMORY_LIMIT_PARAMETER,
        MEMORY_SOFT_LIMIT_PERCENT,
        1,
        1,
        CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
        BATCHWISE2_MINIMUM_BATCHSIZE = 83886080 * 8,
        CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = True,
        COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL
    ))
    
    jobs = []
    for n in batch_numbers:
        job, query_id = queries.run_manual_clustering(session, f"test_table", True, n, n + 1, use_async=True)
        jobs.append(job)
        print(f"Query{n}: {util.query_id_to_snovi_url(query_id)}")
    
    for j in jobs:
        j.result()

def main_1_big_grouped_batch(session):
    queries.create_test_table(session, f"test_table", ORIGINAL_TABLE_NAME)
    queries.reset_table_parameters(session, f"test_table")
    queries.remove_defragmentation(session, f"test_table")
    queries.setup_manual_clustering(session, f"test_table", queries.ClusteringParameters(
        FAN_IN_PARAMETER,
        None,
        MEMORY_SOFT_LIMIT_PERCENT,
        None,
        1,
        CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
        BATCHWISE2_MINIMUM_BATCHSIZE = 83886080 * 8 * 8,
        CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = False,
        COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL,
    ))
    
    job, query_id = queries.run_manual_clustering(session, f"test_table", False, 0, 1, use_async=True)
    print(f"Query: {util.query_id_to_snovi_url(query_id)}")
    
    job.result()


def main_cluster_dop(session):
    queries.reset_session_system_parameters(session)

    for dop in range(1, 9):
        queries.create_test_table(session, "test_table", ORIGINAL_TABLE_NAME)
        queries.reset_table_parameters(session, "test_table")
        queries.remove_defragmentation(session, "test_table")
        queries.setup_manual_clustering(session, "test_table", queries.ClusteringParameters(
            FAN_IN_PARAMETER,
            RSO_MEMORY_LIMIT_PARAMETER,
            MEMORY_SOFT_LIMIT_PERCENT,
            dop,
            BATCH_SIZE_MULTIPLIER_PARAMETER,
            CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
            BATCHWISE2_MINIMUM_BATCHSIZE,
            CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
            COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL

        ))
        _, query_id = queries.run_manual_clustering(session, "test_table", CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED, 0, 1)
        print(util.query_id_to_snovi_url(query_id))


if __name__ == "__main__":
    from src.util.local import get_env_var_config, get_dev_config

    print("Creating session...")
    session = Session.builder.configs(get_dev_config(config_name='tt000006')).create()

    print("Adding import...")
    session.add_packages("pydantic", "pandas")
    session.add_import(typedefs.__file__, 'src.typedefs')
    session.add_import(functions.__file__, 'src.functions')
    session.add_import(util.__file__, 'src.util.util')
    session.add_import(queries.__file__, 'src.queries')

    print("Running queries...")

    command = sys.argv[1]
    if command == "setup":
        main_setup(session, ORIGINAL_TABLE_NAME, 1_000_000_000)
    elif command == "cluster":
        main_cluster(session)
    elif command == "cluster_2x_dop4":
        main_cluster_2x_dop4(session)
    elif command == "cluster_dop":
        main_cluster_dop(session)
    elif command == "8_small_ungrouped_batches":
        main_8_small_ungrouped_batches(session)
    elif command == "1_big_grouped_batch":
        main_1_big_grouped_batch(session)
    elif command == "cluster_auto":
        main_cluster_auto(session)
    else:
        print(f"Invalid command {command}")

    print("Queries complete")
