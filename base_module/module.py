import logging
from abc import ABC, abstractmethod


class Module(ABC):

    @abstractmethod
    def __init__(self, module_name, global_shared_state, log_level):
        logging.basicConfig(level=log_level)
        self._logger = logging.getLogger(module_name)
        self._global_shared_state = lambda: global_shared_state

    @property
    def global_shared_state(self):
        return self._global_shared_state()
