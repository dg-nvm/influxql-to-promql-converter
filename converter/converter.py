from abc import abstractmethod
from base_module.module import Module


class Converter(Module):

    def __init__(
        self,
        module_name,
        global_shared_state,
        log_level,
        error_manager,
    ):
        super().__init__(module_name, global_shared_state, log_level)
        self._error_manager = error_manager

    @abstractmethod
    def convert_dashboard(self, dashboard, meta) -> None:
        """Get's the full dashboard object and meta object like:
        convert_dashboard(dashboard['dashboard'], dashboard['meta'])
        converts in place"""
        pass
