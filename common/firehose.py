from common.aws_resource_base import ResourceBase
from botocore.exceptions import ClientError
import logging
import time

logger = logging.getLogger(__name__)


class Firehose(ResourceBase):
    """  This class is used to interact with AWS Firehose  """

    def __init__(self):
        super().__init__()

    def get_role_arn(self):
        return 'arn:aws:iam::700953316684:role/dlp_firehose'

    def get_bucket_arn(self, tag):
        return 'arn:aws:s3:::{}'.format(tag)

    def get_stream_name(self, tag):
        return '{}-stream'.format(tag)

    def create_s3_dest_config_no_schema(self, tag):
        return {
            'RoleARN': self.get_role_arn(),
            'BucketARN': self.get_bucket_arn(tag),
            'BufferingHints': {
                'SizeInMBs': 64,
                'IntervalInSeconds': 60
            },
            'DataFormatConversionConfiguration': {
                'SchemaConfiguration': {
                    'RoleARN': self.get_role_arn(),
                    'Region': 'us-east-1'
                },
                'InputFormatConfiguration': {
                    'Deserializer': {
                        'OpenXJsonSerDe': {
                            'ConvertDotsInJsonKeysToUnderscores': False,
                            'CaseInsensitive': False,
                        }
                    }
                },
                'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {
                                'Compression': 'SNAPPY',
                                'EnableDictionaryCompression': False,
                                'MaxPaddingBytes': 0
                                }
                            }
                        }
                    }
                }

    def create_s3_dest_config(self, tag, database, table):
        return {
            'RoleARN': self.get_role_arn(),
            'BucketARN': self.get_bucket_arn(tag),
            'BufferingHints': {
                'SizeInMBs': 64,
                'IntervalInSeconds': 60
            },
            'DataFormatConversionConfiguration': {
                'SchemaConfiguration': {
                    'RoleARN': self.get_role_arn(),
                    'DatabaseName': database,
                    'TableName': table,
                    'Region': 'us-east-1'
                },
                'InputFormatConfiguration': {
                    'Deserializer': {
                        'OpenXJsonSerDe': {
                            'ConvertDotsInJsonKeysToUnderscores': False,
                            'CaseInsensitive': False,
                        }
                    }
                },
                'OutputFormatConfiguration': {
                        'Serializer': {
                            'ParquetSerDe': {
                                'Compression': 'SNAPPY',
                                'EnableDictionaryCompression': False,
                                'MaxPaddingBytes': 0
                                }
                            }
                        }
                    }
                }

    def create_stream_wo_schema(self, tag):
        try:
            logger.info("About to create a firehose stream without a schema!")
            self.firehose_client.create_delivery_stream(DeliveryStreamName=self.get_stream_name(tag),
                                                        ExtendedS3DestinationConfiguration=
                                                        self.create_s3_dest_config_no_schema(tag))
        except ClientError:
            logger.exception("Here is the error generated if we don't have a predefined schema within the Glue database"
                             " that Firehose could point to!")

    def check_if_firehose_created(self, tag):
        status = self.firehose_client.describe_delivery_stream(
            DeliveryStreamName=self.get_stream_name(tag))['DeliveryStreamDescription']['DeliveryStreamStatus']
        while status != 'ACTIVE':
            time.sleep(5)
            logger.info("The firehose stream is still being created!")
            status = self.firehose_client.describe_delivery_stream(
                DeliveryStreamName=self.get_stream_name(tag))['DeliveryStreamDescription']['DeliveryStreamStatus']
            continue
        logger.info("The firehose stream has been created!\n")
        return True

    def create_stream(self, tag, database, table):
        """ Creates a Firehose stream to transform JSON data in Apache Parquet data based metadata that exists in
        Athena Glue Catalog """
        try:
            logger.info("About to create a firehose stream ")
            self.firehose_client.create_delivery_stream(DeliveryStreamName=self.get_stream_name(tag),
                                                        ExtendedS3DestinationConfiguration=self.create_s3_dest_config(tag, database, table))
        except ClientError:
            logger.info("The firehose stream has already been created!\n")

    def delete_delivery_stream(self, tag):
        try:
            logger.info("About to delete the firehose stream")
            self.firehose_client.delete_delivery_stream(DeliveryStreamName=self.get_stream_name(tag))
            logger.info("Successfully deleted the firehose stream\n")
        except ClientError:
            logger.info("Firehose stream was already deleted!\n")

    def put_record(self, tag, json_str):
        """ Simulates Glue Firehose to send multiple records to the stream"""
        a = 0
        while a < 2000:
            if a % 100 == 0 and a != 0:
                logger.info("A batch of 100 simple json records have been sent")
            self.firehose_client.put_record(DeliveryStreamName=self.get_stream_name(tag),
                                    Record={
                                    'Data': json_str
                                        }
                                )
            a = a + 1
        logger.info("Records were placed successfully!!")























