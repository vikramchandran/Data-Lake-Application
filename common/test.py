#!/usr/bin/env python3
from common.aws_resource_factory import ResourceFactory
from utility.argument_parser import get_arguments
from utility.commutils import *
import time
import logging
import boto3



"""

This class is used to test if the other classes and methods work properly

"""

logging.basicConfig(level=logging.INFO, format='%(name)s- %(message)s')
logging.getLogger("botocore.credentials").setLevel(logging.ERROR)

resource_factory = ResourceFactory()
aws_s3  = resource_factory.get_resource('s3')
aws_athena = resource_factory.get_resource('athena')
aws_glue = resource_factory.get_resource('glue')
aws_sqs = resource_factory.get_resource('sqs')
aws_firehose = resource_factory.get_resource('firehose')

rawtag = get_arguments().tag
tag = "activity-{}".format(rawtag)
database = "test_database_{}".format(rawtag)
table = "test_table_{}".format(rawtag)


def run_s3_tests():
    aws_s3.create_s3_bucket(tag)
    aws_s3.create_output_bucket(tag)
    # create_test_csv()
    create_simple_json(aws_s3.get_testfile(tag, 'json'))
    # aws_s3.upload_file('csv')
    aws_s3.upload_file(tag, 'json')


def run_glue_tests_1():
    aws_glue.create_classifier(tag)
    aws_glue.create_database(database)
    # aws_glue.create_crawler('csv')
    aws_glue.create_crawler(tag, database,'json')
    aws_glue.start_crawler(database)



def run_glue_tests_2():
    # aws_glue.change_table_name('csv')
    aws_glue.change_table_name(database, table, 'json')


def run_athena_tests():
    aws_athena.create_work_group(tag)
    aws_athena.run_example_1(tag, database, table)
    print_query_result(aws_athena.read_data(tag))
    aws_athena.run_example_2(tag, database, table)
    print_query_result(aws_athena.read_data(tag))
    aws_athena.run_example_3(tag, database, table)
    print_query_result(aws_athena.read_data(tag))

def run_firehose_test():
    aws_firehose.create_stream()


def test_block_1():
    run_s3_tests()
    run_glue_tests_1()

def test_block_2():
    run_glue_tests_2()
    run_athena_tests()

def wipeall():
    aws_s3.delete_s3_bucket(tag)
    aws_s3.delete_output_bucket(tag)
    delete_testfile(aws_s3.get_testfile(tag, 'json'), 'json')
    aws_glue.delete_classifier(tag)
    aws_athena.delete_work_group(tag)
    aws_glue.delete_crawler(database)
    aws_glue.delete_table_wipeout(database, table)
    aws_glue.delete_database(database)



def run_main_test():
    test_block_1()
    logging.info("Finished running block 1")
    time.sleep(2)
    if aws_glue.check_if_crawler_running(database):
        logging.info("About to run block 2")
        aws_glue.get_schema(database, table)
        # test_block_2()



if __name__ == "__main__":
    """"Call run_main_test() to run main tests. Main tests don't print on terminal but you could see your outputs 
    on the console in AWS Sandbox. Buckets are named as "activity-(insert tag name here)." Databases are named 
    "test_database_(insert tag name here)". Tables are named "test_table_(insert tag name here)." Crawlers are
     named "test_database_(insert tag name here)_crawler." Crawler may take a while to run, so it may not be "ready" 
     for several minutes. It takes around 2-8 minutes on average.
    
    After calling run_main_test(), call wipeall() to remove all  resources that were created."""
    jsonfilename = aws_s3.get_testfile(tag, 'json')
    # aws_sqs.send_message(tag, jsonfilename)
    # string = str(aws_sqs.receive_message_body(tag))
    # parsed = json.loads(string)
    # print(json.dumps(parsed, indent=4, sort_keys=True))
    # for i in range(10):
    #     aws_firehose.put_record(tag)
    # aws_firehose.create_stream(tag, database, table)
    # print(boto3.client('sts').get_caller_identity().get('Account'))
    # aws_athena.show_json_extract_fxn(tag, database)
    # print_query_result(aws_athena.read_data(tag))
    create_complex_json(jsonfilename)





















