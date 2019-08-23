from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import logging


logger = logging.getLogger(__name__)


class UC3Step4(BaseStep):

    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.create_upload_print_json(iscomplex=True)
        self.glue_instance.create_database(self.database)
        self.glue_instance.create_classifier(self.tag, self.jsonpath)
        self.glue_instance.create_custom_crawler(self.tag, self.database, 'json')
        logger.info("Notice how I'm using a custom classifier since I'm planning to query a complex json file!\n")

    def run_res(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'json')
            logger.info("Here is how the schema looks after running the Crawler with the custom classifier:")
            print_dict(self.glue_instance.get_schema(self.database, self.table))
            logger.info("Here is the json path for the custom classifier that created the "
                        "schema above: {}\n".format(self.jsonpath))
            logger.info("The schema now is analyzing the records within the json array of the people field"
                        "and not the entire json object itself. Therefore, we either have to perform an ETL job "
                        "to unnest the json file or use custom Presto json functions to query "
                        "this json object\n")
            logger.info("I'm now going to update our json data such that the data is an unnested version of the array within "
                        "the people field\n")
            json_str = get_json_unnested()
            logger.info("Here are the json records that I'm passing into Firehose:\n{}".format(json_str))
            self.firehose_instance.create_stream(self.tag, self.database, self.table)
            if self.firehose_instance.check_if_firehose_created(self.tag):
                self.firehose_instance.put_record(self.tag, json_str)
                logger.info("You should now be able to download a parquet file version of the simple json file "
                            "by going onto the AWS console!")
                logger.info("Firehose saves the parquet file in the bucket,{}, with the follow prefix automatically: "
                            "YYYY/MM/DD/HH/\n".format(self.tag))
