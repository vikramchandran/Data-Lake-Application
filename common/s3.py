from common.aws_resource_base import ResourceBase
from botocore.exceptions import ClientError
import os
import logging


logger = logging.getLogger(__name__)


class S3(ResourceBase):
    """  This class is used to interact with AWS S3  """

    def __init__(self):
        super().__init__()

    def create_s3_bucket_generic(self, name):
        """ Create an Amazon S3 bucket """

        try:
            logger.info("About to create an S3 bucket!")
            self.s3client.create_bucket(Bucket=name)
            logger.info("Created a bucket successfully!\n")
        except ClientError:
            logger.info("There was a client error!!\n")

    def create_s3_bucket(self, tag):
        """ Create an Amazon S3 bucket """
        self.create_s3_bucket_generic(tag)

    def create_output_bucket(self, tag):
        self.create_s3_bucket_generic('{}-results'.format(tag))

    def delete_s3_bucket_generic(self, name):

        """ Deletes an Amazon S3 storage bucket"""
        try:
            self.s3res.meta.client.head_bucket(Bucket=name)
            try:
                logger.info("About to delete the S3 bucket and all of its contents")
                bucket = self.s3res.Bucket(name)
                for key in bucket.objects.all():
                    key.delete()
                bucket.delete()
                logger.info("Deleted the S3 bucket and all of its contents\n")
            except ClientError:
                logger.info("There was an error in deleting the buckets!")
        except ClientError:
            logger.info("The bucket already doesn't exist!\n")

    def delete_s3_bucket(self, tag):
        """ Deletes an Amazon S3 storage bucket"""
        self.delete_s3_bucket_generic(tag)

    def delete_output_bucket(self, tag):
        self.delete_s3_bucket_generic('{}-results'.format(tag))

    def get_testfile_name(self, tag, file_type):
        return '{}/{}-testfile.{}'.format(os.getcwd(), tag, file_type)

    def get_altcsv_name(self, tag):
        return '{}/{}-testfile-altered.csv'.format(os.getcwd(), tag)

    def get_jsonoptional_name(self, tag):
        return '{}/{}-testfile-optional.json'.format(os.getcwd(), tag)

    def upload_file(self, tag, filetype):
        """ Uploads a file to an Amazon S3 storage bucket"""
        filename = 'testfile.{}'.format(filetype)
        new_name = filename.split('/')[-1]
        try:
            logger.info("About to upload the file into the S3 bucket")
            self.s3res.meta.client.upload_file(Filename=self.get_testfile_name(tag, filetype),
                                               Bucket=tag, Key='{}/{}'.format(filetype, new_name))
            logger.info("Uploaded file successfully!\n")
        except ClientError:
            logger.info("There was an error in uploading the file\n")

    def upload_jsonoptional(self, tag):
        """ Uploads a file to an Amazon S3 storage bucket"""
        filename = 'testfile.json'
        new_name = filename.split('/')[-1]
        try:
            logger.info("About to upload the file into the S3 bucket")
            self.s3res.meta.client.upload_file(Filename=self.get_jsonoptional_name(tag),
                                               Bucket=tag, Key='json/{}'.format(new_name))
            logger.info("Uploaded file successfully!\n")
        except ClientError:
            logger.info("There was an error in uploading the file\n")

    def upload_altcsv(self, tag):
        """ Uploads a file to an Amazon S3 storage bucket"""
        filename = 'testfile.csv'
        new_name = filename.split('/')[-1]
        try:
            logger.info("About to upload the file into the S3 bucket")
            self.s3res.meta.client.upload_file(Filename=self.get_altcsv_name(tag),
                                               Bucket=tag, Key='csv/{}'.format(new_name))
            logger.info("Uploaded file successfully!\n")
        except ClientError:
            logger.info("There was an error in uploading the file\n")
