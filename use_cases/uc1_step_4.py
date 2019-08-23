from use_cases.step_generator.base_step import BaseStep
from use_cases.use_case_1.steps.uc1_step_3 import UC1Step3
from utility.commutils import *
import logging
import time

logger = logging.getLogger(__name__)

class UC1Step4(BaseStep):
    def __init__(self):
        super().__init__()
        self.csvaltname = self.s3_instance.get_altcsv_name(self.tag)

    def prep_env(self):
        message = "Going to execute step 3 to place data and run an Athena query!\n"
        UC1Step3().execute_step(message)
        print("\n\n\n\n\n")
        logger.info("Now going to update underlying data and query with Athena again!\n")
        create_test_csv_altered(self.csvaltname)
        self.s3_instance.upload_altcsv(self.tag)
        print_csv(self.csvaltname)
        self.create_glue_resources('csv')

    def run_res(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'csv')
            logger.info("Here is how the updated schema looks after running the same Crawler on the updated data:")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            self.athena_instance.run_example_1(self.tag, self.database, self.table)
            print_query_result(self.athena_instance.read_data(self.tag))
            time.sleep(2)
        logger.info("As you can see, Athena doesn't save the underlying data but only reads based on schema!\n")
        delete_altered_csv(self.csvaltname)




