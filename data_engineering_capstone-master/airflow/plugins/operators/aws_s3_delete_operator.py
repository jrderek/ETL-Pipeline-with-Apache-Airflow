import boto3
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class AWSS3DeleteOperator(BaseOperator):
    """
        A custom airflow operator that deletes objects based on keys or deletes all objects in a
        directory (specified by a prefix) in an AWS S3 Bucket.
    """
    ui_color = '#C87170'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 region,
                 bucket,
                 directories=None,
                 keys=None,
                 *args, **kwargs):

        super(AWSS3DeleteOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.region = region
        self.bucket = bucket
        self.directories = directories
        self.keys = keys

        if self.directories and self.keys:
            raise AirflowException(f"Only one of params `directories` & `keys` have to be specified!")

    def _delete_objects(self, client):
        """
            Deletes Keys from a Bucket.
        """
        if isinstance(self.keys, list):
            keys = self.keys
        else:
            keys = [self.keys]

        delete_dict = {"Objects": [{"Key": k} for k in keys]}
        response = client.delete_objects(Bucket=self.bucket, Delete=delete_dict)

        deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
        self.log.info(f"Deleted: {deleted_keys}")

        if "Errors" in response:
            errors_keys = [x['Key'] for x in response.get("Errors", [])]
            raise AirflowException(f"Errors when deleting: {errors_keys}")

    def execute(self, context):
        """
            Deletes Objects from an S3-Bucket on AWS.
        """
        self.log.info("Initialize AWS connection ...")
        aws_hook = AwsHook(self.conn_id)
        credentials = aws_hook.get_credentials()

        client = boto3.client("s3",
                              region_name=self.region,
                              aws_access_key_id=credentials.access_key,
                              aws_secret_access_key=credentials.secret_key
                              )

        # delete all keys in directories
        if self.directories and not self.keys:
            for d in self.directories:
                self.log.info(f"Deleting all contents from directory {d} in bucket {self.bucket} ...")
                response = client.list_objects(Bucket=self.bucket, Prefix=d)
                self.keys = [con["Key"] for con in response["Contents"]]
                self._delete_objects(client)

        # delete defined keys only
        elif self.keys and not self.directories:
            self.log.info(f"Deleting defined objects {self.keys} from bucket {self.bucket} ...")
            self._delete_objects(client)
