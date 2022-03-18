import pendulum
from datetime import datetime, timedelta

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.ssh_operator import SSHOperator


local_tz = pendulum.timezone("Europe/Amsterdam")

# set dag default-args
default_args = {
    "owner": "dnks23",
    "start_date": datetime(2020, 7, 3, 10, 0, 0, tzinfo=local_tz),
    "email_on_retry": False,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "catchup": False,
    "max_active_runs": 1
}

# set lists of spark-job-names
ingest_staging_spark_job_names = ["ingest_staging_temperature", "ingest_staging_airport",
                                  "ingest_staging_demographic", "ingest_staging_immigration"]

process_static_dimension_spark_job_names = ["process_static_dimension_port", "process_static_dimension_travel_mode",
                                            "process_static_dimension_visa_type"]

process_fact_spark_job_names = ["process_fact_immigration"]

process_dimension_spark_job_names = ["process_dimension_date", "process_dimension_airport",
                                     "process_dimension_state", "process_dimension_country"]


# define data quality checks
data_quality_checks = [{"sql": "SELECT * FROM dim_visa_type WHERE id IS NULL;",
                        "expected_result": 0},
                       {"sql": "SELECT * FROM dim_travel_mode WHERE id IS NULL;",
                        "expected_result": 0},
                       {"sql": "SELECT * FROM dim_port WHERE id IS NULL;",
                        "expected_result": 0},
                       {"sql": "SELECT count(*) FROM (SELECT DISTINCT state_id FROM dim_state);",
                        "expected_result": 55},
                       {"sql": "SELECT count(*) FROM dim_date;",
                        "expected_result": 366},
                       {"sql": "SELECT * FROM dim_airport WHERE (state_id IS NULL) OR (airport_code IS NULL);",
                        "expected_result": 0},
                       {"sql": "SELECT * FROM dim_country WHERE country_id IS NULL;",
                       "expected_result": 0},
                       {"sql": "SELECT * FROM fact_immigration WHERE (port_id IS NULL) OR (state_id IS NULL);",
                        "expected_result": 1}]


def build_spark_submit(job_type, app_name, job_path, input_path="", output_path="", table_sink="",
                       jdbc_conn_id="", tmp_path=""):
    """
        Builds a spark-submit command based on given params.
    """
    ALLOWED_JOB_TYPES = ["ingest", "process"]

    if job_type not in ALLOWED_JOB_TYPES:
        raise ValueError(f"Param `job_type` has to be one of {ALLOWED_JOB_TYPES}.")

    if job_type == "ingest":
        
        spark_submit = f"spark-submit --deploy-mode cluster --name {app_name} " \
                       f"--packages 'saurfang:spark-sas7bdat:2.1.0-s_2.11," \
                       f"org.apache.hadoop:hadoop-aws:2.7.2' " \
                       f"--py-files /home/hadoop/python/util/python.py {job_path} " \
                       f"--input {input_path} --output {output_path} --table-sink {table_sink}"
    
    elif job_type == "process":
        
        jdbc_uri = get_redshift_uri(jdbc_conn_id)
        spark_submit = f"spark-submit --deploy-mode cluster --name {app_name} " \
                       f"--jars /home/hadoop/resources/redshift-jdbc42-1.2.41.1065.jar " \
                       f"--packages 'saurfang:spark-sas7bdat:2.1.0-s_2.11," \
                       f"com.databricks:spark-redshift_2.11:2.0.1," \
                       f"org.apache.hadoop:hadoop-aws:2.7.2," \
                       f"org.apache.spark:spark-avro_2.11:2.4.0' " \
                       f"--py-files /home/hadoop/python/util/python.py {job_path} " \
                       f"--jdbc-uri '{jdbc_uri}' " \
                       f"--table-sink {table_sink} --tmp {tmp_path}"

        if input_path != "":
            spark_submit += f" --input {input_path}"
        if output_path != "":
            spark_submit += f" --output {output_path}"

    return spark_submit


def get_redshift_uri(conn_id):
    """
        Builds a (redshift-) jdbc-uri from a given airflow connection-id.
    """
    hook = BaseHook(conn_id)
    conn = hook.get_connection(conn_id)
    if not conn.host:
        return ""
    else:
        uri = f"jdbc:redshift://{conn.host}:{conn.port}/{conn.schema}?user={conn.login}&password={conn.password}"
        extra = conn.extra_dejson
        params = [f"{k}={v}" for k, v in extra.items()]

        if params:
            params = "&".join(params)
            uri += f"?{params}"

        return uri


def get_ssh_operator(job_type, _name, _dag, _ssh_conn_id, _aws_s3_spark_config, _table_sink, _jdbc_conn_id=None):
    """
        Creates a list of ssh-operators with given params.
    """
    ALLOWED_JOB_TYPES = ["ingest", "process"]

    if job_type not in ALLOWED_JOB_TYPES:
        raise ValueError(f"Param `job_type` has to be one of {ALLOWED_JOB_TYPES}.")
    elif job_type == "ingest":
        ssh_operator = SSHOperator(
            task_id=_name,
            dag=_dag,
            ssh_conn_id=_ssh_conn_id,
            command=build_spark_submit(job_type=job_type, app_name=_name,
                                       job_path=f"{_aws_s3_spark_config['spark_jobs']}/{_name}.py",
                                       input_path=_aws_s3_spark_config['s3_ingest'],
                                       output_path=_aws_s3_spark_config['s3_raw'], table_sink=_table_sink
                                       )
        )
    elif job_type == "process":
        ssh_operator = SSHOperator(
            task_id=_name,
            dag=_dag,
            ssh_conn_id=_ssh_conn_id,
            command=build_spark_submit(job_type=job_type, app_name=_name,
                                       job_path=f"{_aws_s3_spark_config['spark_jobs']}/{_name}.py",
                                       jdbc_conn_id=_jdbc_conn_id,
                                       table_sink=_table_sink, tmp_path=_aws_s3_spark_config['s3_tmp'],
                                       input_path=_aws_s3_spark_config['s3_res'],
                                       output_path=_aws_s3_spark_config['s3_processed']
                                       )
        )

    return ssh_operator
