from use_cases.step_generator.base_step import BaseStep
from use_cases.use_case_1.steps.uc1_step_1 import UC1Step1
from use_cases.use_case_1.steps.uc1_step_2 import UC1Step2
from use_cases.use_case_1.steps.uc1_step_3 import UC1Step3
from use_cases.use_case_1.steps.uc1_step_4 import UC1Step4
from use_cases.use_case_2.steps.uc2_step_1 import UC2Step1
from use_cases.use_case_2.steps.uc2_step_2 import UC2Step2
from use_cases.use_case_2.steps.uc2_step_3 import UC2Step3
from use_cases.use_case_2.steps.uc2_step_4 import UC2Step4
from use_cases.use_case_2.steps.uc2_step_5 import UC2Step5
from use_cases.use_case_3.steps.uc3_step_1 import UC3Step1
from use_cases.use_case_3.steps.uc3_step_2 import UC3Step2
from use_cases.use_case_3.steps.uc3_step_3 import UC3Step3
from use_cases.use_case_3.steps.uc3_step_4 import UC3Step4


class StepFactory(object):
    def __init__(self, use_case):
        self.use_case = use_case

    def raise_generic_notimplementedstep(self, use_case, step):
        raise NotImplementedError("Step {} hasn't been implemented yet for use case {} or the step factory class hasn't"
                                  " been updated to handle step {} within use case {}. Please ask a developer to "
                                  "update this referring to the documentation on Confluence.".format(step, use_case,
                                                                                                    step, use_case))

    def raise_generic_notimplementedusecase(self, use_case):
        raise NotImplementedError("Use case {} hasn't been implemented yet or the step factory class hasn't been "
                                  "updated to handle use case {}. Please ask a developer to update this "
                                  "referring to the documentation on Confluence.".format(use_case, use_case))

    def get_step(self, step):
        if step == 0:
            return BaseStep()
        if self.use_case == 1:
            return self.handle_use_case_1(step)
        elif self.use_case == 2:
            return self.handle_use_case_2(step)
        elif self.use_case == 3:
            return self.handle_use_case_3(step)
        else:
            self.raise_generic_notimplementedusecase(self.use_case)

    def handle_use_case_1(self, step):
        if step == 1:
            return UC1Step1()
        elif step == 2:
            return UC1Step2()
        elif step == 3:
            return UC1Step3()
        elif step == 4:
            return UC1Step4()
        else:
            self.raise_generic_notimplementedstep(1, step)

    def handle_use_case_2(self, step):
        if step == 1:
            return UC2Step1()
        elif step == 2:
            return UC2Step2()
        elif step == 3:
            return UC2Step3()
        elif step == 4:
            return UC2Step4()
        elif step == 5:
            return UC2Step5()
        else:
            self.raise_generic_notimplementedstep(2, step)

    def handle_use_case_3(self, step):
        if step == 1:
            return UC3Step1()
        elif step == 2:
            return UC3Step2()
        elif step == 3:
            return UC3Step3()
        elif step == 4:
            return UC3Step4()
        else:
            self.raise_generic_notimplementedstep(3, step)


