import boto3
from datetime import datetime
import os

from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AWSCreateClusterOperator(BaseOperator):
    """
        A custom airflow operator that creates AWS-Redshift or EMR -Clusters by using boto3 library with
        given configs from Airflow Variables.
    """

    ui_color = "#9CCBA2"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 cluster_type="emr",
                 time_zone=None,
                 *args,
                 **kwargs):
        super(AWSCreateClusterOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.cluster_type = cluster_type
        self.time_zone = time_zone

    def execute(self, context):
        """
            Creates a EMR/Redshift-cluster on AWS.
        """
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()
        
        if self.cluster_type == "emr":
            # get config variable based on cluster-type
            config = Variable.get("aws_emr_cluster_config", default_var={}, deserialize_json=True)
            client = boto3.client(self.cluster_type,
                                  region_name=config["region"],
                                  aws_access_key_id=credentials.access_key,
                                  aws_secret_access_key=credentials.secret_key
                                  )
            # create emr-cluster
            self.log.info(f"Creating EMR-Cluster ...")
            response = client.run_job_flow(
                Name=f"{config['cluster_name']}-airflow-{datetime.now(self.time_zone).strftime('%Y-%m-%d-%H-%M-%S')}",
                ReleaseLabel=config["release_label"],
                Applications=[
                    {"Name": "Hadoop"},
                    {"Name": "Spark"},
                    {"Name": "Hive"},
                    {"Name": "Livy"}
                ],
                Instances={
                    "InstanceGroups": [
                        {
                            "Name": "Master nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "MASTER",
                            "InstanceType": config["master_instance_type"],
                            "InstanceCount": 1
                        },
                        {
                            "Name": "Slave nodes",
                            "Market": "ON_DEMAND",
                            "InstanceRole": "CORE",
                            "InstanceType": config["slave_node_instance_type"],
                            "InstanceCount": config["num_slave_nodes"]
                        }
                    ],
                    "Ec2KeyName": config["ec2_key_name"],
                    "KeepJobFlowAliveWhenNoSteps": True,
                    "TerminationProtected": False,
                },
                VisibleToAllUsers=True,
                JobFlowRole="EMR_EC2_DefaultRole",
                ServiceRole="EMR_DefaultRole",
                BootstrapActions=[
                    {
                        "Name": config["bootstrap"]["name"],
                        "ScriptBootstrapAction": {
                            "Path": config["bootstrap"]["path"]
                        }
                    },
                ],
                Configurations=[
                    {
                        'Classification': "spark-env",
                        'Configurations': [
                            {
                                "Classification": "export",
                                "Properties": {
                                    "PYSPARK_PYTHON": "/usr/bin/python3"
                                }
                            }
                        ]
                    },
                ]
            )

            if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
                raise AirflowException(f"EMR-Cluster creation failed: {response}")

            else:
                self.log.info(f"Cluster {response['JobFlowId']} created with params: {response}")
                return response["JobFlowId"]

        elif self.cluster_type == "redshift":
            # get config variable based on cluster-type
            config = Variable.get("aws_redshift_cluster_config", default_var={}, deserialize_json=True)
            client = boto3.client(self.cluster_type,
                                  region_name=config["region"],
                                  aws_access_key_id=credentials.access_key,
                                  aws_secret_access_key=credentials.secret_key
                                  )

            # get custom redshift-db config from environment-variables
            db_name = os.environ["AWS_REDSHIFT_SCHEMA"]
            master_user = os.environ["AWS_REDSHIFT_USER"]
            master_pw = os.environ["AWS_REDSHIFT_PW"]
            # create emr-cluster
            self.log.info(f"Creating Redshift-Cluster ...")
            response = client.create_cluster(
                ClusterIdentifier=f"{config['cluster_identifier']}-airflow-"
                                  f"{datetime.now(self.time_zone).strftime('%Y-%m-%d-%H-%M-%S')}",
                ClusterType=config["cluster_type"],
                NodeType=config["node_type"],
                NumberOfNodes=config["num_nodes"],
                DBName=db_name,
                MasterUsername=master_user,
                MasterUserPassword=master_pw
            )

            if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
                raise AirflowException(f"Redshift-Cluster creation failed: {response}")

            else:
                self.log.info(f"Cluster {response['Cluster']['ClusterIdentifier']} created with params: {response}")
                return response["Cluster"]["ClusterIdentifier"]

        else:
            raise ValueError(f"Provided cluster-type is not supported!")
