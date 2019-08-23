from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import logging


logger = logging.getLogger(__name__)


class UC2Step5(BaseStep):

    def __init__(self):
        super().__init__()
        self.jsonoptfilename = self.s3_instance.get_jsonoptional_name(self.tag)

    def prep_env(self):
        self.create_s3_buckets()
        self.athena_instance.create_work_group(self.tag)
        self.glue_instance.create_classifier(self.tag, self.jsonpath)
        create_test_json_optional(self.jsonoptfilename)
        self.s3_instance.upload_jsonoptional(self.tag)
        print_json_optional(self.jsonoptfilename)
        logger.info("Notice how there are optional attributes in the json file uploaded to S3!\n")
        self.create_glue_resources('json')

    def run_res_without_custom(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the schema looks after running the Crawler without the custom classifier:\n")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            logger.info("As you can see, the schema generated doesn't include specific columns about our data\n")

    def clean_res(self):
        self.glue_instance.delete_database(self.database)
        self.glue_instance.delete_crawler(self.database)
        self.glue_instance.create_database(self.database)
        self.glue_instance.create_custom_crawler(self.tag, self.database, 'json')

    def run_res_with_custom(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the schema looks after running the Crawler with the custom classifier:")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            logger.info("Here is the json path for the custom classifier that created the "
                        "schema above: {}\n".format(self.jsonpath))
            logger.info("The metadata above shows the specific columns within the json data!\n")
            logging.info("Also, notice how optional attributes are not missed by the schema!\n")

    def run_res(self):
        self.run_res_without_custom()
        self.clean_res()
        self.run_res_with_custom()
        delete_opttestfile_json(self.jsonoptfilename)