# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from task.tasks import Emm


def main():
    Emm.delay()
