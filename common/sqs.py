import logging
from common.aws_resource_base import ResourceBase
from botocore.exceptions import ClientError
import json


logger = logging.getLogger(__name__)


class SQS(ResourceBase):
    """  This class is used to interact with AWS SQS  """

    def __init__(self):
        super().__init__()

    def __get_queue_name(self, tag):
        return 'sqs-{}'.format(tag)

    def create_queue(self, tag):
        try:
            logger.info("About to create a SQS queue")
            self.sqs_client.create_queue(QueueName=self.__get_queue_name(tag))
            logger.info("Successfully created a SQS queue")
        except ClientError:
            logger.info("There was an error in creating the SQS queue")

    def get_queue_url(self, tag):
        print(self.__get_queue_name(tag))
        response = self.sqs_client.get_queue_url(QueueName=self.__get_queue_name(tag))
        return response['QueueUrl']

    def send_message(self, tag, jsonfilename):
        try:
            logger.info("About to send a message through SQS queue")
            with open(jsonfilename, 'r',) as f:
                file = json.load(f)
                file = json.dumps(file)
                self.sqs_client.send_message(QueueUrl=self.get_queue_url(tag), MessageBody=file)
            logger.info("Successfully sent a message through SQS queue")
        except ClientError:
            logger.info("Couldn't send a message through SQS queue!\n")

    def receive_message(self, tag):
        return self.sqs_client.receive_message(QueueUrl=self.get_queue_url(tag))['Messages']

    def receive_message_body(self,tag):
        return self.receive_message(tag)[0]['Body']

    def delete_queue(self, tag):
        try:
            logger.info("About to delete any previously created SQs queue")
            self.sqs_client.delete_queue(QueueUrl=self.get_queue_url(tag))
            logger.info("SQS queue successfully deleted!\n")
        except ClientError:
            logger.info("Queue has already been deleted")





