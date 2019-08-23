from use_cases.step_generator.base_step import BaseStep
from utility.commutils import *
import logging


logger = logging.getLogger(__name__)


class UC3Step1(BaseStep):

    def __init__(self):
        super().__init__()

    def prep_env(self):
        self.create_s3_buckets()
        self.create_upload_print_json(iscomplex=False)
        self.firehose_instance.create_stream_wo_schema(self.tag)

    def run_res(self):
        pass



