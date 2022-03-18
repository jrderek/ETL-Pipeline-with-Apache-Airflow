import boto3

from airflow.models import Variable
from airflow.exceptions import AirflowException, AirflowConfigException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


ALLOWED_CLUSTER_TYPES = ["emr", "redshift"]


class AWSTerminateClusterOperator(BaseOperator):
    """
        A custom airflow operator that terminates AWS-Redshift or EMR-Clusters by using boto3 library with
        given configs from Airflow Variables.
    """
    ui_color = "#CBA29C"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 cluster_creation_task,
                 cluster_type=None,
                 *args,
                 **kwargs):
        super(AWSTerminateClusterOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.cluster_creation_task = cluster_creation_task
        self.cluster_type = cluster_type

        if self.cluster_type not in ALLOWED_CLUSTER_TYPES:
            raise AirflowConfigException(f"Param `cluster_type` must be one of {ALLOWED_CLUSTER_TYPES}")

    def execute(self, context):
        """
            Terminates a EMR-/Redshift-cluster on AWS.
        """
        self.log.info("Get cluster-id ...")
        cc_dag_task = self.cluster_creation_task.split(".")
        cluster_id = context["ti"].xcom_pull(dag_id=cc_dag_task[0], task_ids=cc_dag_task[1])

        if not cluster_id:
            self.log.info(f"Could not get cluster-id! Looks like there was no cluster started ...")
        else:

            self.log.info("Initialize AWS connection ...")
            aws_hook = AwsHook(self.conn_id)
            credentials = aws_hook.get_credentials()

            # terminate emr-cluster
            if self.cluster_type == "emr":
                # get config variable based on cluster-type
                config = Variable.get("aws_redshift_cluster_config", default_var={}, deserialize_json=True)
                client = boto3.client(self.cluster_type,
                                      region_name=config["region"],
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key
                                      )
                self.log.info(f"Terminate EMR-cluster with ID {cluster_id} on AWS ...")
                response = client.terminate_job_flows(JobFlowIds=[cluster_id])

                if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    raise AirflowException(f"EMR-Cluster termination failed: {response}")

                else:
                    self.log.info(f"Cluster with id {cluster_id} terminated!")

            # terminate redshift-cluster
            elif self.cluster_type == "redshift":
                # get config variable based on cluster-type
                config = Variable.get("aws_redshift_cluster_config", default_var={}, deserialize_json=True)
                client = boto3.client(self.cluster_type,
                                      region_name=config["region"],
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key
                                      )
                self.log.info(f"Terminate Redshift-cluster with ID {cluster_id} on AWS ...")
                response = client.delete_cluster(ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)

                if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    raise AirflowException(f"Redshift-Cluster termination failed: {response}")

                else:
                    self.log.info(f"Cluster with id {cluster_id} terminated!")

            else:
                raise ValueError(f"Provided cluster-type is not supported!")



