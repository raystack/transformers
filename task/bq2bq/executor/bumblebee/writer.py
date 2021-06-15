import json
import os
import pathlib
from abc import ABC
from abc import abstractmethod

from bumblebee.log import get_logger

logger = get_logger(__name__)


class BaseWriter(ABC):

    @abstractmethod
    def write(self, key: str, value: str):
        pass


class JsonWriter(BaseWriter):

    def __init__(self, filepath: str):
        self.filepath = filepath
        # create dir if not already exists
        pathlib.Path(os.path.dirname(self.filepath)).mkdir(parents=True, exist_ok=True)
        return

    def write(self, key: str, value: str):
        try:
            data_file = open(self.filepath, 'r')
            data = json.load(data_file)
            data_file.close()
        except FileNotFoundError as e:
            # do nothing, fresh file
            data = {}

        data[key] = value
        logger.debug("{}: {}".format(key, value))

        with open(self.filepath, 'w') as the_file:
            json.dump(data, the_file)

        logger.info(data)
        return


class StdWriter(BaseWriter):
    def write(self, key: str, value: str):
        logger.info("{}: {}".format(key, value))
        return
