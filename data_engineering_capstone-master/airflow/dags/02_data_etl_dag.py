import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from util.airflow import default_args, ingest_staging_spark_job_names,\
    process_static_dimension_spark_job_names, process_fact_spark_job_names,\
    process_dimension_spark_job_names, get_ssh_operator
from sensors.aws_cluster_creation_sensor import AWSClusterCreationSensor
from operators.airflow_connection_operator import AirflowConnectionOperator
from operators.aws_s3_delete_operator import AWSS3DeleteOperator


local_tz = pendulum.timezone("Europe/Amsterdam")

# set configs & create variables
DAG_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
AWS_BUCKET_NAME = os.environ["AWS_S3_BUCKET"]
AWS_CONN_ID = "aws_credentials"
AWS_EMR_SSH_CONN_ID = "aws_emr_ssh"
AWS_REDSHIFT_CONN_ID = "aws_redshift_db"

aws_s3_spark_config = Variable.get("aws_s3_spark_config", default_var={}, deserialize_json=True)
ingest_staging_spark_job_tasks = []
process_fact_spark_job_tasks = []
process_dimension_spark_job_tasks = []
process_static_dimension_spark_job_tasks = []

dag = DAG(DAG_NAME,
          default_args=default_args,
          description="Loads and transforms data on AWS.",
          schedule_interval="0 * * * *"
          )


wait_on_emr_cluster_creation = AWSClusterCreationSensor(
    task_id="wait_on_emr_cluster_creation",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_creation_task="01_provision_cluster_dag.create_emr_cluster",
    cluster_type="emr",
    mode="reschedule",
    poke_interval=120
    )

create_emr_ssh_connection = AirflowConnectionOperator(
    task_id="create_emr_ssh_connection",
    dag=dag,
    conn_id=AWS_EMR_SSH_CONN_ID,
    conn_type="ssh",
    custom_aws_conn=True,
    custom_conn_params={"aws_conn_id": AWS_CONN_ID,
                        "aws_config_key": "aws_emr_cluster_config",
                        "aws_cluster_type": "emr",
                        "cluster_creation_task": "01_provision_cluster_dag.create_emr_cluster"}
    )


wait_on_redshift_cluster_creation = AWSClusterCreationSensor(
    task_id="wait_on_redshift_cluster_creation",
    dag=dag,
    conn_id=AWS_CONN_ID,
    cluster_creation_task="01_provision_cluster_dag.create_redshift_cluster",
    cluster_type="redshift",
    mode="reschedule",
    poke_interval=120
    )

update_redshift_db_connection = AirflowConnectionOperator(
    task_id="update_redshift_db_connection",
    dag=dag,
    conn_id=AWS_REDSHIFT_CONN_ID,
    conn_type="postgres",
    custom_aws_conn=True,
    custom_conn_params={"aws_conn_id": AWS_CONN_ID,
                        "aws_config_key": "aws_redshift_cluster_config",
                        "aws_cluster_type": "redshift",
                        "cluster_creation_task": "01_provision_cluster_dag.create_redshift_cluster"}
    )


cluster_provisioned = DummyOperator(
    task_id="cluster_provisioned",
    dag=dag
    )


for _name in ingest_staging_spark_job_names:
    _table_sink = _name.split("_", 1)[1].replace("staging", "stag")
    _task = get_ssh_operator("ingest", _name, dag, AWS_EMR_SSH_CONN_ID,
                             aws_s3_spark_config, _table_sink, AWS_REDSHIFT_CONN_ID)
    ingest_staging_spark_job_tasks.append(_task)


ingest_completed = DummyOperator(
    task_id="ingest_completed",
    dag=dag
    )

for _name in process_static_dimension_spark_job_names:
    _table_sink = _name.split("_", 2)[2].replace("dimension", "dim")
    _task = get_ssh_operator("process", _name, dag, AWS_EMR_SSH_CONN_ID,
                             aws_s3_spark_config, _table_sink, AWS_REDSHIFT_CONN_ID)
    process_static_dimension_spark_job_tasks.append(_task)


static_dimensions_completed = DummyOperator(
    task_id="static_dimensions_completed",
    dag=dag
    )


for _name in process_fact_spark_job_names:
    _table_sink = _name.split("_", 1)[1]
    _task = get_ssh_operator("process", _name, dag, AWS_EMR_SSH_CONN_ID,
                             aws_s3_spark_config, _table_sink, AWS_REDSHIFT_CONN_ID)
    process_fact_spark_job_tasks.append(_task)


facts_completed = DummyOperator(
    task_id="facts_completed",
    dag=dag
    )


for _name in process_dimension_spark_job_names:
    _table_sink = _name.split("_", 1)[1].replace("dimension", "dim")
    _task = get_ssh_operator("process", _name, dag, AWS_EMR_SSH_CONN_ID,
                             aws_s3_spark_config, _table_sink, AWS_REDSHIFT_CONN_ID)
    process_dimension_spark_job_tasks.append(_task)


dimensions_completed = DummyOperator(
    task_id="dimensions_completed",
    dag=dag
    )

s3_clean_up = delete_from_s3_task = AWSS3DeleteOperator(
    task_id="s3_clean_up",
    dag=dag,
    conn_id=AWS_CONN_ID,
    region=aws_s3_spark_config["region"],
    bucket=aws_s3_spark_config["s3_bucket_name"],
    directories=["capstone/data/tmp/"]
)

end_task = DummyOperator(
    task_id="end_task",
    dag=dag
    )


# set dependencies
wait_on_emr_cluster_creation.set_downstream(create_emr_ssh_connection)
wait_on_redshift_cluster_creation.set_downstream(update_redshift_db_connection)
cluster_provisioned.set_upstream([update_redshift_db_connection, create_emr_ssh_connection])
cluster_provisioned.set_downstream(ingest_staging_spark_job_tasks)
ingest_completed.set_upstream(ingest_staging_spark_job_tasks)
ingest_completed.set_downstream(process_static_dimension_spark_job_tasks)
static_dimensions_completed.set_upstream(process_static_dimension_spark_job_tasks)
static_dimensions_completed.set_downstream(process_fact_spark_job_tasks)
facts_completed.set_upstream(process_fact_spark_job_tasks)
facts_completed.set_downstream(process_dimension_spark_job_tasks)
dimensions_completed.set_upstream(process_dimension_spark_job_tasks)
s3_clean_up.set_upstream(dimensions_completed)
end_task.set_upstream(s3_clean_up)
