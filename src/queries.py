from snowflake.snowpark.session import Session
from snowflake.snowpark.dataframe import col, DataFrame
import re
from typing import Optional

class ClusteringParameters:
    def __init__(self,
                FAN_IN_PARAMETER,
                RSO_MEMORY_LIMIT_PARAMETER,
                MEMORY_SOFT_LIMIT_PERCENT,
                DOP_PARAMETER,
                BATCH_SIZE_MULTIPLIER_PARAMETER,
                CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
                BATCHWISE2_MINIMUM_BATCHSIZE,
                CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
                COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL,
        ):
        self.FAN_IN_PARAMETER = FAN_IN_PARAMETER
        self.RSO_MEMORY_LIMIT_PARAMETER = RSO_MEMORY_LIMIT_PARAMETER
        self.MEMORY_SOFT_LIMIT_PERCENT = MEMORY_SOFT_LIMIT_PERCENT
        self.DOP_PARAMETER = DOP_PARAMETER
        self.BATCH_SIZE_MULTIPLIER_PARAMETER = BATCH_SIZE_MULTIPLIER_PARAMETER
        self.CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT = CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT
        self.BATCHWISE2_MINIMUM_BATCHSIZE = BATCHWISE2_MINIMUM_BATCHSIZE
        self.CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED
        self.COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL

def setup_queries():
    return """
create or replace database tlee_test;
use database tlee_test;
create schema test;
use schema test;
create or replace warehouse tlee_wh_xs warehouse_size = xsmall server_type='c6gd2xlarge';
create or replace warehouse tlee_wh_xxxl warehouse_size = xxxlarge server_type='c6gd2xlarge';
use warehouse tlee_wh_xs;
select pi();
"""

def reset_session_system_parameters_queries():
    return """
alter session unset
    rows_per_rowset,
    MIN_ROWS_PER_ROWSET,
    fdn_one_rowset_per_file,
    max_files_to_gc,
    FDN_FILE_SIZE,
    clustering_internal,
    RSO_MEMORY_LIMIT,
    SORT_MAX_TOTAL_MERGE_FAN_IN,
    MEMORY_SOFT_LIMIT,
    ALLOW_ANY_LOCAL_DOP;
   
alter system unset
    CLUSTERING_MAX_NUM_CONSECUTIVE_GC_RUNS,
    CLUSTERING_MIN_NUM_CLUSTERING_RUNS_BEFORE_NEXT_GC,
    AUTO_CLUSTERING_POLICY_STANDARD_MAX_BATCHES_PER_ROUND,
    TXN_MAX_TABLE_LOCK_WAITERS,
    CLUSTERING_EXECUTION_USE_TRANSACTIONAL_SUCCESSOR_TASK_GENERATOR,
    compute_service_task_dequeue_service_frequency_ms,
    COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL;
"""

def reset_table_parameters_queries(table_name: str):
    return f"""
alter table {table_name} unset
    enable_fdn_file_defragmentation,
    CLUSTERING_SERVICE_TRIGGER_GC_PCNT_THRESHOLD,
    CLUSTERING_SERVICE_TRIGGER_GC_THRESHOLD,
    clustering_execution_scheduler_enabled,
    clustering_store_batch_info,
    clustering_sort_batches_by_row_count,
    CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED,
    CLUSTERING_EXECUTION_SCHEDULER_MAX_TASKS,
    CLUSTERING_SERVICE_BATCH_SIZE_MULTIPLIER,
    enable_clustering_service,
    FDN_FILE_SIZE,
    CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT,
    BATCHWISE2_MINIMUM_BATCHSIZE;
"""

def create_table_queries(fdn_file_size: Optional[int], name: str, num_rows: int):
    result = ""
    if fdn_file_size:
        result += f"alter session set FDN_FILE_SIZE = {fdn_file_size} parameter_comment = 'feature testing';"
    result += f"""
    create or replace table {name}(a float, b int);
    delete from {name};
    use warehouse tlee_wh_xxxl;
    insert into {name} select normal(10, 5, random()) a, random() b from table(generator(rowcount => {num_rows}));
    use warehouse tlee_wh_xs;
    """
    return result

def create_test_table_queries(table_name: str, original_table_name: str):
    return f"create or replace table {table_name} clone {original_table_name};"

def remove_defragmentation_queries(table_name: str):
    return f"""
    alter session set max_files_to_gc = 0 parameter_comment='feature testing';
    alter system set CLUSTERING_MAX_NUM_CONSECUTIVE_GC_RUNS = 0 parameter_comment = 'feature testing';
    alter system set CLUSTERING_MIN_NUM_CLUSTERING_RUNS_BEFORE_NEXT_GC = 10000 parameter_comment = 'feature testing';

    alter table {table_name} set enable_fdn_file_defragmentation = false parameter_comment = 'feature testing';
    alter table {table_name} set CLUSTERING_SERVICE_TRIGGER_GC_PCNT_THRESHOLD = 1.0 parameter_comment = 'feature testing';
    alter table {table_name} set CLUSTERING_SERVICE_TRIGGER_GC_THRESHOLD = 2100000000 parameter_comment = 'feature testing';
    """

def _set_clustering_parameters_queries(table_name: str, clustering_parameters: ClusteringParameters):
    result = ""

    result += f"""
    alter system set AUTO_CLUSTERING_POLICY_STANDARD_MAX_BATCHES_PER_ROUND = 1000 parameter_comment='feature testing';
    alter system set TXN_MAX_TABLE_LOCK_WAITERS = 1000 parameter_comment='feature testing';

    alter system set CLUSTERING_EXECUTION_USE_TRANSACTIONAL_SUCCESSOR_TASK_GENERATOR = true parameter_comment = 'feature testing';

    alter table {table_name} set clustering_execution_scheduler_enabled = true parameter_comment='feature testing';
    alter table {table_name} set clustering_store_batch_info = true parameter_comment='feature testing';
    alter table {table_name} set clustering_sort_batches_by_row_count = true parameter_comment='feature testing';
    """

    if clustering_parameters.COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL:
        result += f"alter system set COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL = '{clustering_parameters.COMPUTE_SERVICE_WAREHOUSE_CLUSTERING_EXECUTION_POOL}' parameter_comment='feature testing';"

    if clustering_parameters.CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED:
        result += f"alter table {table_name} set CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED = true parameter_comment='feature testing';"

    result += f"""
    alter table {table_name} set CLUSTERING_EXECUTION_SCHEDULER_MAX_TASKS = 20 parameter_comment='feature testing';
    """
    if clustering_parameters.BATCH_SIZE_MULTIPLIER_PARAMETER is None:
        result += f"alter table {table_name} unset CLUSTERING_SERVICE_BATCH_SIZE_MULTIPLIER;"
    else:
        result += f"alter table {table_name} set CLUSTERING_SERVICE_BATCH_SIZE_MULTIPLIER = {clustering_parameters.BATCH_SIZE_MULTIPLIER_PARAMETER} parameter_comment='feature testing';"

    if clustering_parameters.CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT is None:
        result += f"alter table {table_name} unset CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT;"
    else:
        result += f"alter table {table_name} set CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT = {clustering_parameters.CLUSTERING_SERVICE_BATCHSET_SIZE_LIMIT} parameter_comment='feature testing';"

    if clustering_parameters.BATCHWISE2_MINIMUM_BATCHSIZE is None:
        result += f"alter table {table_name} unset BATCHWISE2_MINIMUM_BATCHSIZE;"
    else:
        result += f"alter table {table_name} set BATCHWISE2_MINIMUM_BATCHSIZE = {clustering_parameters.BATCHWISE2_MINIMUM_BATCHSIZE} parameter_comment='feature testing';"

    result += f"""
    alter table {table_name} set enable_clustering_service = false parameter_comment='feature testing';
    alter table {table_name} cluster by (a);
    alter table {table_name} set enable_clustering_service = true parameter_comment='feature testing';
    """

    if clustering_parameters.FAN_IN_PARAMETER is not None and clustering_parameters.DOP_PARAMETER is not None:
        result += f"alter session set SORT_MAX_TOTAL_MERGE_FAN_IN = {clustering_parameters.FAN_IN_PARAMETER * clustering_parameters.DOP_PARAMETER};"
    
    if clustering_parameters.RSO_MEMORY_LIMIT_PARAMETER is None:
        result += "alter session unset RSO_MEMORY_LIMIT;"
    else:
        result += f"alter session set RSO_MEMORY_LIMIT = '{clustering_parameters.RSO_MEMORY_LIMIT_PARAMETER}';"

    if clustering_parameters.MEMORY_SOFT_LIMIT_PERCENT is None:
        result += "alter session unset MEMORY_SOFT_LIMIT;"
    else:
        result += f"alter session set MEMORY_SOFT_LIMIT = '{clustering_parameters.MEMORY_SOFT_LIMIT_PERCENT}%';"
    
    if clustering_parameters.DOP_PARAMETER is not None and clustering_parameters.CLUSTERING_EXECUTION_GROUPED_BATCHES_ENABLED:
        result += f"""
        alter session set ALLOW_ANY_LOCAL_DOP=true;
        alter session set local_dop = {clustering_parameters.DOP_PARAMETER} parameter_comment='feature testing';
        alter session set ENABLE_DOP_DOWNGRADE = false;

        alter session set GLOBAL_DOP_OVERRIDE = {clustering_parameters.DOP_PARAMETER} parameter_comment='feature testing';
        """

    return result

def setup_manual_clustering_queries(table_name: str, clustering_parameters: ClusteringParameters):
    result = ""

    result += f"""
    alter system set compute_service_task_dequeue_service_frequency_ms = -1 parameter_comment='feature testing';
    alter session set clustering_internal = true;
    """

    result += _set_clustering_parameters_queries(table_name, clustering_parameters)

    result += f"""
    alter table {table_name} resume recluster;
    select system$clustering_service_select_files('{table_name}', false);
    """

    return result

def run_manual_clustering_queries(table_name: str, is_grouped: bool, start_batch: int, end_batch: int):
    return f"alter table {table_name} recluster execute_only=true {'grouped_batch=true' if is_grouped else ''} start_batch={start_batch} end_batch={end_batch};"

def setup_auto_clustering_queries(table_name: str, clustering_parameters: ClusteringParameters):
    return _set_clustering_parameters_queries(table_name, clustering_parameters)

def run_auto_clustering_queries(table_name):
    return f"alter table {table_name} resume recluster;"

def setup_automatic_clustering_queries(table_name: str, clustering_parameters: ClusteringParameters):
    result = ""
    result += _set_clustering_parameters_queries(table_name, clustering_parameters)
    result += f"alter table {table_name} resume recluster;"

    return result


def run_queries(session: Session, query_string: str, use_async = False):
    result = None, None
    for query in re.split("(?<=;)", query_string):
        query = query.strip()

        if not query:
            continue
            
        if query[-1] != ";":
            print(f"Invalid query: {query}")
            return None

        print(f"{query}")
        job = session.sql(query.strip()).collect_nowait()
        result = job if use_async else job.result(), job.query_id
    return result



def setup(session: Session) -> DataFrame:
    print("\n### setup")
    return run_queries(session, setup_queries())

def reset_session_system_parameters(session: Session) -> DataFrame:
    print("\n### reset_session_system_parameters")
    return run_queries(session, reset_session_system_parameters_queries())

def create_original_table(session: Session, fdn_file_size: int, table_name: str, num_rows: int):
    print("\n### create_original_table")
    return run_queries(session, create_table_queries(fdn_file_size, table_name, num_rows))

def create_test_table(session: Session, table_name: str, original_table_name):
    print("\n### create_test_table")
    return run_queries(session, create_test_table_queries(table_name, original_table_name))

def reset_table_parameters(session: Session, table_name: str):
    print("\n### reset_table_parameters")
    return run_queries(session, reset_table_parameters_queries(table_name))

def remove_defragmentation(session: Session, table_name: str):
    print("\n### remove_defragmentation")
    return run_queries(session, remove_defragmentation_queries(table_name))

def setup_manual_clustering(session: Session, table_name: str, clustering_parameters: ClusteringParameters):
    print("\n### setup_manual_clustering")
    return run_queries(session, setup_manual_clustering_queries(table_name, clustering_parameters))

def run_manual_clustering(session: Session, table_name: str, is_grouped, start_batch, end_batch, use_async = False):
    print("\n### run_manual_clustering")
    return run_queries(session, run_manual_clustering_queries(table_name, is_grouped, start_batch, end_batch), use_async)

def setup_auto_clustering(session: Session, table_name: str, clustering_parameters: ClusteringParameters):
    print("\n### setup_auto_clustering")
    return run_queries(session, setup_auto_clustering_queries(table_name, clustering_parameters))

def run_auto_clustering(session: Session, table_name: str, use_async = False):
    print("\n### run_manual_clustering")
    res =  run_queries(session, run_auto_clustering_queries(table_name), use_async)

    table_info = run_queries(session, f"select system$dict_id('table', '{table_name}');")
    print(table_info)

    return res