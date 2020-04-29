# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SelfCollectingFramework",
    "SelfCollectingFrameworkMeta"
]

import abc
import importlib
from glob import glob
from pathlib import Path
from typing import Union, Callable, List


class SelfCollectingFrameworkMeta(abc.ABCMeta):

    def __init__(cls, name, bases, attrs):

        super().__init__(name, bases, attrs)

        # 记录父类的引用
        cls.BASECLASS = None
        if bases:
            cls.BASECLASS = bases[-1]


class SelfCollectingFramework(metaclass=SelfCollectingFrameworkMeta):
    """子类自收集框架"""

    # 需要索引的路径
    # TODO 路径必须是绝对路径
    PATH_TO_IMPORT: Union[str, List[str], Callable] = None

    # 执行import时文件相对路径的前缀
    RELATIVE_IMPORT_TOP_PATH_PREFIX: str = None

    # 需要收集的子类
    COLLECTED: ["SelfCollectingFramework"] = []

    @classmethod
    def need_collect(cls):
        """装饰需要收集的子类"""
        def inner(model):
            # 只能检测子类，并不能检测直接子类
            assert issubclass(model, cls)
            if model not in cls.COLLECTED:
                cls.COLLECTED.append(model)
            return model
        return inner

    @classmethod
    @abc.abstractmethod
    def process(cls, collected=None, **kwargs):
        if collected is None:
            collected = cls.COLLECTED

    @classmethod
    def collect(cls):
        """通过文件目录路径，import相应的module"""
        print(f"collecting modules for {cls.__doc__} ...")
        if callable(cls.PATH_TO_IMPORT):
            dirs = cls.PATH_TO_IMPORT()
        else:
            dirs = cls.PATH_TO_IMPORT
        if isinstance(dirs, str):
            dirs = [dirs]
        elif isinstance(dirs, (tuple, list)):
            pass
        else:
            assert 0
        module_dirs = []
        for the_dir in dirs:
            module_dirs += glob(
                str(Path(the_dir) / "**.py"),
                recursive=True
            )
            module_dirs += glob(
                str(Path(the_dir) / "**/**.py"),
                recursive=True
            )
        for module_dir in module_dirs:
            relative_path = module_dir[len(cls.RELATIVE_IMPORT_TOP_PATH_PREFIX):]
            # TODO only support *nix system
            py_file_dot_split = [i for i in relative_path.split("/") if i]
            py_file_path_for_importing = ".".join(py_file_dot_split)[:-3]
            importlib.import_module(f"{py_file_path_for_importing}")
