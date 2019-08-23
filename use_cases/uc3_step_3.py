from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import logging


logger = logging.getLogger(__name__)


class UC3Step3(BaseStep):

    def __init__(self):
        super().__init__()


    def prep_env(self):
        self.create_s3_buckets()
        create_nested_json(self.jsonfilename)
        self.s3_instance.upload_file(self.tag, 'json')
        print_json_complex(self.jsonfilename)
        self.glue_instance.create_database(self.database)
        self.glue_instance.create_crawler(self.tag, self.database, 'json')
        logger.info("We're not using any custom classifier for this trial\n")

    def run_res(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the schema looks after running the Crawler without a custom classifier:")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            self.firehose_instance.create_stream(self.tag, self.database, self.table)
            if self.firehose_instance.check_if_firehose_created(self.tag):
                json_str = get_json_str(self.jsonfilename)
                self.firehose_instance.put_record(self.tag, json_str)
                logger.info("Firehose is able to successfully transform complex json data by just using the built-in "
                            "json classifier for our Crawler! There is no need to write a custom classifier at all. "
                            "Therefore, a user only has to deploy the Crawler on the complex json file, point Firehose "
                            "to the where the metadata lives in Glue Catalog, and Firehose should successfully be able"
                            "to transform the complex json file to Parquet while streaming it! ")
                logger.info("You should now be able to download a parquet file version of the simple json file "
                            "by going onto the AWS console!")
                logger.info("Firehose saves the parquet file in the bucket,{}, with the follow prefix automatically: "
                            "YYYY/MM/DD/HH/\n".format(self.tag))

