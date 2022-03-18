import os
import pendulum

from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from util.airflow import default_args
from operators.aws_create_cluster_operator import AWSCreateClusterOperator
from operators.aws_terminate_cluster_operator import AWSTerminateClusterOperator


local_tz = pendulum.timezone("Europe/Amsterdam")

# set configs & create variables
DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
AWS_CONN_ID = "aws_credentials"

dag = DAG(DAG_NAME,
          default_args=default_args,
          description="Creates and terminates AWS resources.",
          schedule_interval="0 * * * *"
          )


create_emr_cluster = AWSCreateClusterOperator(
    task_id="create_emr_cluster",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_type="emr",
    time_zone=local_tz
    )


create_redshift_cluster = AWSCreateClusterOperator(
    task_id="create_redshift_cluster",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_type="redshift",
    time_zone=local_tz
    )


wait_on_tasks_completed = ExternalTaskSensor(
    task_id="wait_on_tasks_completed",
    dag=dag,
    external_dag_id="03_data_quality_check_dag",
    external_task_id="end_task",
    mode="reschedule",
    poke_interval=120
    )


terminate_emr_cluster = AWSTerminateClusterOperator(
    task_id="terminate_emr_cluster",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_creation_task=DAG_NAME + ".create_emr_cluster",
    cluster_type="emr"
    )

terminate_redshift_cluster = AWSTerminateClusterOperator(
    task_id="terminate_redshift_cluster",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_creation_task=DAG_NAME + ".create_redshift_cluster",
    cluster_type="redshift"
    )


# set dependencies
wait_on_tasks_completed.set_upstream([create_emr_cluster, create_redshift_cluster])
wait_on_tasks_completed.set_downstream([terminate_emr_cluster, terminate_redshift_cluster])
