from use_cases.step_generator.base_step import BaseStep
import logging
from utility.commutils import delete_testfile


logger = logging.getLogger(__name__)


class UC1Step1(BaseStep):
    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.athena_instance.create_work_group(self.tag)
        self.create_upload_print_csv()

    def run_res(self):
        self.athena_instance.run_example_1(self.tag, self.database, self.table)
        self.athena_instance.show_query_failed(self.tag)
        delete_testfile(self.csvfilename, 'csv')

