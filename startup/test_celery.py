# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from task.tasks import ATestTask


def main():
    ATestTask.shoot()
