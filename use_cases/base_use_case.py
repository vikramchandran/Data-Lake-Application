from use_cases.step_generator.step_factory import StepFactory

class BaseUseCase(object):
    def __init__(self, use_case, step_id):
        self.use_case = use_case
        self.step_id = step_id
        self.use_case_factory = StepFactory(use_case)

    def execute(self):
        step = self.use_case_factory.get_step(self.step_id)
        message ="About to now create the resources necessary to " \
                 "run all resources for use case {}, step {}\n".format(self.use_case, self.step_id)
        step.execute_step(message)

    def wipe_out(self):
        step = self.use_case_factory.get_step(0)
        step.wipe_out_env()


