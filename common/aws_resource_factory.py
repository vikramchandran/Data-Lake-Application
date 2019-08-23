from common.athena import Athena
from common.firehose import Firehose
from common.glue import Glue
from common.s3 import S3
from common.sqs import SQS


class ResourceFactory(object):
    def get_resource(self, resource_name):
        if resource_name == 'athena':
            return Athena()
        elif resource_name == 'glue':
            return Glue()
        elif resource_name == 'firehose':
            return Firehose()
        elif resource_name == 's3':
            return S3()
        elif resource_name == 'sqs':
            return SQS()
        else:
            raise NameError("No such resource exists!")
