from common.aws_resource_factory import ResourceFactory
from utility.argument_parser import get_arguments
from utility.commutils import *

class BaseStep(object):
    def __init__(self):
        self.s3_instance, self.glue_instance, self.athena_instance, self.firehose_instance = self.get_resources()
        self.tag, self.database, self.table = self.get_main_params()
        self.jsonfilename, self.csvfilename = self.get_names()
        self.jsonpath = self.glue_instance.create_custom_jsonpath()

    def _generic_notimplement(self, function_name):
        raise NotImplementedError("Step didn't have {} method implemented! Please have a developer implement it "
                                  "according to the documentation on Confluence.".format(function_name))

    def prep_env(self):
        self._generic_notimplement("prep_env")

    def run_res(self):
        self._generic_notimplement("run_res")

    # Test if these systems do work in terms of wiping out
    def wipe_out_env(self):
        self.s3_instance.delete_s3_bucket(self.tag)
        self.s3_instance.delete_output_bucket(self.tag)
        self.athena_instance.delete_work_group(self.tag)
        self.glue_instance.delete_database(self.database)
        self.glue_instance.delete_crawler(self.database)
        delete_testfile(self.csvfilename, 'csv')
        delete_testfile(self.jsonfilename, 'json')
        self.glue_instance.delete_classifier(self.tag)
        self.csvaltname = self.s3_instance.get_altcsv_name(self.tag)
        delete_altered_csv(self.csvaltname)
        self.jsonoptfilename = self.s3_instance.get_jsonoptional_name(self.tag)
        delete_opttestfile_json(self.jsonoptfilename)
        self.firehose_instance.delete_delivery_stream(self.tag)

    def get_main_params(self):
        rawtag = get_arguments().tag
        tag = "dlp-{}".format(rawtag)
        database = "test_database_{}".format(rawtag)
        table = "test_table_{}".format(rawtag)
        return tag, database, table

    def get_resources(self):
        resource_factory = ResourceFactory()
        s3_instance = resource_factory.get_resource("s3")
        glue_instance = resource_factory.get_resource("glue")
        athena_instance = resource_factory.get_resource("athena")
        firehose_instance = resource_factory.get_resource("firehose")
        return s3_instance, glue_instance, athena_instance, firehose_instance

    def get_names(self):
        jsonfilename = self.s3_instance.get_testfile_name(self.tag, 'json')
        csvfilename = self.s3_instance.get_testfile_name(self.tag, 'csv')
        return jsonfilename, csvfilename

    def execute_step(self, message):
        logger.info(message)
        self.prep_env()
        self.run_res()

    def create_s3_buckets(self):
        self.s3_instance.create_s3_bucket(self.tag)
        self.s3_instance.create_output_bucket(self.tag)

    def create_upload_print_csv(self):
        create_test_csv(self.csvfilename)
        self.s3_instance.upload_file(self.tag, 'csv')
        print_csv(self.csvfilename)

    def create_upload_print_json(self, iscomplex):
        if iscomplex:
            create_test_json_complex(self.jsonfilename)
            self.s3_instance.upload_file(self.tag, 'json')
            print_json_complex(self.jsonfilename)
        else:
            create_test_json_simple(self.jsonfilename)
            self.s3_instance.upload_file(self.tag, 'json')
            print_json_simple(self.jsonfilename)

    def create_glue_resources(self, type):
        self.glue_instance.create_database(self.database)
        self.glue_instance.create_crawler(self.tag, self.database, type)

    def create_glue_table(self):
        self.glue_instance.start_crawler(self.database)
        if self.glue_instance.check_if_crawler_running(self.database):
            self.glue_instance.change_table_name(self.database, self.table, 'csv')



