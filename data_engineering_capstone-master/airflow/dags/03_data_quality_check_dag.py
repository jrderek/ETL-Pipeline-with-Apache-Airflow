import os
import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from util.airflow import default_args, data_quality_checks
from operators.aws_redshift_data_quality_operator import AWSRedshiftDataQualityOperator


local_tz = pendulum.timezone("Europe/Amsterdam")

# set configs & create variables
DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
AWS_REDSHIFT_CONN_ID = "aws_redshift_db"


dag = DAG(DAG_NAME,
          default_args=default_args,
          description="Performs data quality checks.",
          schedule_interval="0 * * * *"
          )


wait_on_data_model_creation = ExternalTaskSensor(
    task_id="wait_on_data_model_creation",
    dag=dag,
    external_dag_id="02_data_etl_dag",
    external_task_id="end_task",
    mode="reschedule",
    poke_interval=120
)


run_data_quality_checks = AWSRedshiftDataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    conn_id=AWS_REDSHIFT_CONN_ID,
    checks=data_quality_checks
)


end_task = DummyOperator(
    task_id="end_task",
    dag=dag
    )

# set dependencies
wait_on_data_model_creation.set_downstream(run_data_quality_checks)
run_data_quality_checks.set_downstream(end_task)
