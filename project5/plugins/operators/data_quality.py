from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 column="",
                 skip=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.column = column

    def execute(self, context):

        if self.skip:
            return

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Check Null Value")
        query = self.query.format(self.table)
        records = redshift.get_records(query)
        if records[0] > 0:
            raise ValueError("Data quality check failed. Songplays - {} has null values".format(self.column))
