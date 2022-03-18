from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSRedshiftDataQualityOperator(BaseOperator):
    """
        A custom airflow operator that performs prior defined data quality checks
        against data in aws redshift. check queries need to be pre-defined as python dict,
        e.g. {'sql': "your test-sql-statement", 'expected_result': the expected result}
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 checks=[],
                 *args, **kwargs):

        super(AWSRedshiftDataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.checks = checks

    def execute(self, context):
        """
            Performs defined checks against a redshift database to assure data quality checks.
        """
        self.log.info('Initialize connection to database ...')
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info(self.checks)
        for check in self.checks:
            self.log.info(f"Check data quality  ...")
            self.log.info(f"Query to check: {check['sql']} ...")
            self.log.info(f"Expected result: {check['expected_result']} ...")

            records = redshift_hook.get_records(f"{check['sql']}")
            num_records = len(records)
            result = None

            if num_records != 0:
                result = records[0][0]

            if num_records != check['expected_result'] and result != check['expected_result']:
                raise ValueError(
                    f"Data quality check failed! Test {check['sql']} returned {num_records} records & result {result}. "
                    f"Expected result {check['expected_result']} is not met!")

            self.log.info(f"Test {check['sql']} passed successfully with {num_records} records / result {result}!")
