import logging
import sys

import utility.argument_parser as argument_parser
from use_cases.base_use_case import BaseUseCase


arguments = argument_parser.get_arguments()

# root_logger = logging.getLogger()
# root_logger.setLevel(logging.DEBUG)
#
# stream = logging.StreamHandler()
# stream.setLevel(logging.INFO)
# root_logger.addHandler(stream)
#
# logger = logging.getLogger(__name__)
# logger.info(sys.path)

logging.basicConfig(level=logging.INFO, format='%(name)s- %(message)s')
logging.getLogger("botocore.credentials").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class Application(object):
    def __init__(self, args):
        self.use_case_id, self.step_id = self.get_config(args)

    def get_config(self, args):
        use_case_id = int(args.use_case_id)
        step_id = int(args.step_id)
        return use_case_id, step_id

    def execute(self):
        self.execute_helper(self.use_case_id, self.step_id)

    def execute_helper(self, use_case_id, step_id):
        use_case = BaseUseCase(use_case_id, step_id)
        logger.info("About to wipe out any resources that may have previously been created!\n\n\n")
        use_case.wipe_out()
        logger.info("Going to execute the specified use case and step right now\n\n\n\n\n\n")
        use_case.execute()

    def log_text(self):
        text = """Buckets are named as "dlp-(insert tag name here)"  Databases are named 
        "test_database_(insert tag name here)". Tables are named "test_table_(insert tag name here)."
        Crawlers are named "test_database_(insert tag name here)_crawler." Outputs/ Results of Athena queries will
        be in csv format (since that is the default for Athena query results) and will be stored in the bucket
        "dlp-(insert tag name here)-result." Firehose streams are also named "dlp-(insert tag name here)-stream."
        All resources that are created could be seen on the AWS console, currently under AWS Sandbox. 
        There will also be logs printed below that states exactly what is going on 
         at every step. \n """
        logger.info(text)

if __name__ == '__main__':
    arguments = argument_parser.get_arguments()
    application = Application(arguments)
    application.log_text()
    application.execute()

    # try:
    #     application.run()
    # except Exception as e:
    #     for line in str(e).split('\n'):
    #         logger.error(line)
    #     raise e
