import boto3
from sqlalchemy.orm import exc

from airflow import settings
from airflow.models import Variable, Connection
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class AirflowConnectionOperator(BaseOperator):
    """
        A custom Airflow Operator that is able to create a new or update a given Airflow Connection with new params.
    """

    ui_color = "#DEEFE0"

    @apply_defaults
    def __init__(self,
                 conn_id,
                 conn_type=None,
                 conn_host=None,
                 conn_port=None,
                 conn_login=None,
                 conn_password=None,
                 conn_schema=None,
                 conn_extra=None,
                 custom_aws_conn=False,
                 custom_conn_params=None,
                 *args,
                 **kwargs):
        super(AirflowConnectionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.conn_type = conn_type
        self.conn_host = conn_host
        self.conn_port = conn_port
        self.conn_login = conn_login
        self.conn_password = conn_password
        self.conn_schema = conn_schema
        self.conn_extra = conn_extra
        self.custom_aws_conn = custom_aws_conn
        self.custom_conn_params = custom_conn_params

        if not self.conn_id or self.conn_id == "":
            raise AirflowException(f"Required param `conn_id` is missing!")

    def _check_if_connection_exists(self, session, conn_id):
        """
            Checks if a connection already exists.
        """
        try:
            query = (session
                     .query(Connection)
                     .filter(Connection.conn_id == conn_id)
                     .one())

        except exc.NoResultFound:
            self.log.info(f"Did not find a connection with `conn_id`={conn_id}")
            return False
        except exc.MultipleResultsFound:
            self.log.info(f"Found more than one connection with `conn_id`={conn_id}")
            return "multiple"

        if query:
            self.log.info(f"Connection with `conn_id` = {conn_id} already exists!")
            return True

    def _update_connection(self, session, conn_id):
        """
            Updates an existing connection.
        """
        existing_connection = (session
                               .query(Connection)
                               .filter(Connection.conn_id == conn_id)
                               .one())

        new_connection = existing_connection

        if self.conn_type and self.conn_type != existing_connection.conn_type:
            new_connection.conn_type = self.conn_type

        if self.conn_host and self.conn_host != existing_connection.host:
            new_connection.host = self.conn_host

        if self.conn_port and self.conn_port != existing_connection.port:
            new_connection.port = self.conn_port

        if self.conn_login and self.conn_login != existing_connection.login:
            new_connection.login = self.conn_login

        if self.conn_password and self.conn_password != existing_connection.password:
            new_connection.password = self.conn_password

        if self.conn_schema and self.conn_schema != existing_connection.schema:
            new_connection.schema = self.conn_schema

        if self.conn_extra and self.conn_extra != existing_connection.extra:
            new_connection.extra = self.conn_extra

        self.log.info(f"Deleting existing connection ...")
        session.delete(existing_connection)

        self.log.info(f"Creating new connection with `conn_id`={conn_id} ...")
        session.add(new_connection)

    def execute(self, context):
        """
            Creates or updates an existing airflow connection.
        """

        self.log.info("Initialize connection params ...")
        session = settings.Session()

        if self.custom_aws_conn:
            self.log.info(f"Custom AWS Connection requested! Pulling Cluster-Identifier "
                          f"from XCom and initialize AWS connection ...")
            aws_hook = AwsHook(self.custom_conn_params["aws_conn_id"])
            credentials = aws_hook.get_credentials()

            cc_dag_task = self.custom_conn_params["cluster_creation_task"].split(".")
            cluster_id = context["ti"].xcom_pull(dag_id=cc_dag_task[0], task_ids=cc_dag_task[1])
            self.log.info(f"Fetched Cluster-ID {cluster_id}")

            config = Variable.get(self.custom_conn_params["aws_config_key"], default_var={}, deserialize_json=True)
            client = boto3.client(self.custom_conn_params["aws_cluster_type"],
                                  region_name=config["region"],
                                  aws_access_key_id=credentials.access_key,
                                  aws_secret_access_key=credentials.secret_key
                                  )

            # get connection host based on cluster-type, only emr/redshift are supported
            if self.custom_conn_params["aws_cluster_type"] == "emr":
                self.conn_host = client.describe_cluster(ClusterId=cluster_id)["Cluster"]["MasterPublicDnsName"]

            elif self.custom_conn_params["aws_cluster_type"] == "redshift":
                self.conn_host = client.describe_clusters(ClusterIdentifier=
                                                          cluster_id)["Clusters"][0]["Endpoint"]["Address"]
            else:
                raise AirflowException(f"Provided `aws_cluster_type` not supported!")

        exists = self._check_if_connection_exists(session, self.conn_id)
        if exists:
            self.log.info(f"Update existing connection ... overwriting provided information ...")
            self._update_connection(session, self.conn_id)

        elif not exists:
            self.log.info(f"Creating a new connection with `conn_id` = {self.conn_id} ...")

            new_connection = Connection(
                conn_id=self.conn_id,
                conn_type=self.conn_type,
                host=self.conn_host,
                port=self.conn_port,
                login=self.conn_login,
                password=self.conn_password,
                schema=self.conn_schema,
                extra=self.conn_extra
            )

            session.add(new_connection)

        elif exists == "multiple":
            session.commit()
            raise AirflowException(f"Handling of multiple existing connections currently not supported ... Aborting!")

        session.commit()
