import boto3

from airflow.models import Variable
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AWSClusterCreationSensor(BaseSensorOperator):
    """
        A custom Airflow Sensor that pokes the status of a AWS-Redshift or AWS-EMR Cluster and waits until the Cluster
        is ready to use.
    """

    ui_color = "#B9C6DA"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 cluster_creation_task="create_cluster",
                 cluster_type="emr",
                 *args,
                 **kwargs):
        super(AWSClusterCreationSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.cluster_creation_task = cluster_creation_task
        self.cluster_type = cluster_type

    def poke(self, context):
        """
            Waits until a redshift- or emr-cluster started by a given task is up & running.
        """
        self.log.info("Get cluster-id ...")
        cc_dag_task = self.cluster_creation_task.split(".")
        cluster_id = context["ti"].xcom_pull(dag_id=cc_dag_task[0], task_ids=cc_dag_task[1])
        
        self.log.info(f"Poking for cluster created by {cc_dag_task[0]}.{cc_dag_task[1]} ...")
        if not cluster_id:
            self.log.info(f"Could not get cluster-id! Looks like there was no cluster started ...")
            return False
        else:
            self.log.info(f"Cluster with id {cluster_id} was started ...")
            self.log.info("Initialize AWS connection ...")
            aws_hook = AwsHook(self.conn_id)
            credentials = aws_hook.get_credentials()

            # get status of emr-cluster
            if self.cluster_type == "emr":
                config = Variable.get("aws_emr_cluster_config", default_var={}, deserialize_json=True)
                client = boto3.client(self.cluster_type,
                                      region_name=config["region"],
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key
                                      )

                cluster_properties = client.describe_cluster(ClusterId=cluster_id)
                if cluster_properties["Cluster"]["Status"]["State"] == "WAITING":
                    self.log.info(f"EMR-Cluster with id {cluster_id} is up and waiting ...")
                    return True

            # get status of redshift-cluster
            if self.cluster_type == "redshift":
                config = Variable.get("aws_redshift_cluster_config", default_var={}, deserialize_json=True)
                client = boto3.client(self.cluster_type,
                                      region_name=config["region"],
                                      aws_access_key_id=credentials.access_key,
                                      aws_secret_access_key=credentials.secret_key
                                      )

                cluster_properties = client.describe_clusters(ClusterIdentifier=cluster_id)
                if cluster_properties['Clusters'][0]['ClusterStatus'] == "available":
                    self.log.info(f"Redshift-Cluster with id {cluster_id} is up and available ...")
                    return True

            self.log.info(f"Cluster with id {cluster_id} not yet ready ...")
            return False
