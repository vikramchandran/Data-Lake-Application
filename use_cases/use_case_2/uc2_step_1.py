from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import logging
import time


logger = logging.getLogger(__name__)


class UC2Step1(BaseStep):

    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.athena_instance.create_work_group(self.tag)
        self.create_upload_print_json(iscomplex=False)
        self.create_glue_resources('json')

    def run_res(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the the schema looks after running the Crawler:")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            self.athena_instance.run_example_1(self.tag, self.database, self.table)
            print_query_result(self.athena_instance.read_data(self.tag))
            time.sleep(2)
            logger.info("""You could also see the query output on the AWS Sandbox console under the "
                        'dlp-(insert tag name here)-result' S3 bucket as a csv file\n""")
        delete_testfile(self.jsonfilename, 'json')






