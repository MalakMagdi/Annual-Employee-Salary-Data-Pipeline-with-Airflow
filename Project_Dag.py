import sys
sys.path.append('/opt/airflow/includes')
from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from datetime import datetime
from emp_dim_insert_update import join_and_detect_new_or_changed_rows
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from queries import INSERT_INTO_DWH_EMP_DIM
from queries import UPDATE_DWH_EMP_DIM
from airflow.operators.python import BranchPythonOperator


def check_row_count(ti):
    rows_to_insert = ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows", key="ids_to_update")
    if len(rows_to_insert) > 0:
        return "update_into_Dimension"
    else:
        return "Insert_into_Dimension"

with DAG("Project", start_date=datetime(2023, 5, 14), schedule_interval='@yearly', catchup=False) as dag:
    sql_to_s3_task1 = SqlToS3Operator(
        task_id="GetDataFromSource1",
        sql_conn_id="PostGres_GetData",
        aws_conn_id="AWS_GET_SOURCE",
        query="select * from finance.emp_sal",
        s3_bucket="staging.emp.data",
        s3_key="<your_file>.csv",
        replace=True,
    )

    sql_to_s3_task2 = SqlToS3Operator(
        task_id="GetDataFromSource2",
        sql_conn_id="PostGres_GetData",
        aws_conn_id="AWS_GET_SOURCE",
        query="select * from hr.emp_details",
        s3_bucket="staging.emp.data",
        s3_key="<your_file>.csv",
        replace=True,
    )

    Insert_into_Dimension = SnowflakeOperator(
        task_id="Insert_into_Dimension",
        snowflake_conn_id='SnowFlake_Connection_Project',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows",key="rows_to_insert")}}')
    )

    update_into_Dimension = SnowflakeOperator(
        task_id="update_into_Dimension",
        snowflake_conn_id='SnowFlake_Connection_Project',
        sql=UPDATE_DWH_EMP_DIM(
            '{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows",key="ids_to_update")}}')
    )

    check_row_count_task = BranchPythonOperator(
        task_id="check_row_count",
        python_callable=check_row_count,
        provide_context=True
    )

    Insert_AfterUpdate = SnowflakeOperator(
        task_id="Insert_AfterUpdate",
        snowflake_conn_id='SnowFlake_Connection_Project',
        sql=INSERT_INTO_DWH_EMP_DIM('{{ti.xcom_pull(task_ids="join_and_detect_new_or_changed_rows",key="rows_to_insert")}}'),
        trigger_rule="none_failed"
    )


    [sql_to_s3_task1 , sql_to_s3_task2] >> join_and_detect_new_or_changed_rows() >> check_row_count_task
    check_row_count_task >> Insert_into_Dimension
    check_row_count_task >> update_into_Dimension >>  Insert_AfterUpdate

