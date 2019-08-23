import argparse



def get_arguments():
    parser = argparse.ArgumentParser()
    tag = parser.add_mutually_exclusive_group(required=True)
    tag.add_argument("--tag", help="name of person using dlp-poc")
    use_case_id = parser.add_mutually_exclusive_group(required=True)
    use_case_id.add_argument("--use-case-id", help="use case to run for DLP")
    step_id = parser.add_mutually_exclusive_group(required=True)
    step_id.add_argument("--step-id", help="step to execute within that use case")
    args = parser.parse_args()
    return args

