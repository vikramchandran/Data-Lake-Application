from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import time
import logging


logger = logging.getLogger(__name__)


class UC2Step3(BaseStep):

    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.athena_instance.create_work_group(self.tag)
        self.create_upload_print_json(iscomplex=True)
        self.create_glue_resources('json')

    def run_res(self):
        self.athena_instance.run_example_generic_helper_1(self.tag, self.database)
        time.sleep(2)
        print_dict(self.glue_instance.get_schema(self.database, self.table))
        self.athena_instance.run_example_generic_helper_2(self.tag, self.database)
        time.sleep(2)
        self.athena_instance.run_example_generic(self.tag, self.database)
        logger.info("As you can see, querying complex json data through the generic table "
                    "approach requires use of Presto json functions like element_at and json_parse!"
                    " The queries are also longer than the explicit table approach explained in "
                    " step 2. \n")
        print_query_result(self.athena_instance.read_data(self.tag))
        time.sleep(2)
        logger.info("""You could also see the query output on the AWS Sandbox console under the "
                    'dlp-(insert tag name here)-result' S3 bucket as a csv file\n""")
        delete_testfile(self.jsonfilename, 'json')



