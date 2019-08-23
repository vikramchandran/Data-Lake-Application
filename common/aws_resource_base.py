import boto3


class ResourceBase(object):
    """
    This class is used to create a Session object that is necessary to interact with AWS

    """
    def __init__(self):
        self.s3res, self.s3client, self.athena_client, self.glue_client, self.sqs_client, self.firehose_client = self.get_resources()

    def get_resources(self):
        session = boto3.Session(profile_name='sandbox')
        s3res = session.resource('s3')
        s3client = session.client('s3')
        athena_client = session.client('athena', region_name='us-east-1')
        glue_client = session.client('glue', region_name='us-east-1')
        sqs_client = session.client('sqs', region_name='us-east-1')
        firehose_client= session.client('firehose', region_name='us-east-1')
        return [s3res, s3client, athena_client, glue_client, sqs_client, firehose_client]








