from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import time
import logging


logger = logging.getLogger(__name__)


class UC2Step4(BaseStep):

    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.athena_instance.create_work_group(self.tag)
        self.glue_instance.create_classifier(self.tag, self.jsonpath)
        self.create_upload_print_json(iscomplex=True)
        self.glue_instance.create_database(self.database)
        self.glue_instance.create_custom_crawler(self.tag, self.database, 'json')

    def run_res(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the schema looks after running the Crawler with the custom classifier:\n")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            logger.info("Here is the json path for the custom classifier that created the "
                        "schema above: {}\n".format(self.jsonpath))
            logger.info("The metadata above shows the specific fields within the json data!\n")
        delete_testfile(self.jsonfilename, 'json')