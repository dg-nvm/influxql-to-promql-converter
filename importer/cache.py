from pathlib import Path
from pickle import Pickler, Unpickler
import sys


class GeneralCache:
    PROTOCOL_VERSION = 4

    def __init__(self, correctness_check, filepath=".cache"):
        self._cache_file = Path(filepath).resolve()
        p = self._cache_file.parent
        if p.exists() is False:
            p.mkdir(parents=True)
        self._check = correctness_check

    def _valid_objects(self, objects: list):
        return self._check(objects)

    def cache_available(self) -> bool:
        if self._cache_file.is_file():
            try:
                objects = self._unpickle()
            except (TypeError, EOFError):
                return False
            return self._valid_objects(objects)
        return False

    def _unpickle(self) -> list:
        unpickler = Unpickler(self._cache_file.open("rb"))
        return unpickler.load()

    def load(self) -> list:
        sys.stderr.write(
            f"!!!! Using {self.__class__.__name__} from {self._cache_file.resolve()} !!!!\n"
        )
        sys.stderr.flush()
        return self._unpickle()

    def save(self, objects: list) -> bool:
        pickler = Pickler(self._cache_file.open("wb"), self.PROTOCOL_VERSION)
        if self._valid_objects(objects):
            pickler.dump(objects)
            return True
        else:
            raise ValueError("Invalid objects passed to cache")


class DashboardsCache(GeneralCache):
    def __init__(self, filepath=".dashboards-cache"):
        super().__init__(self._valid_dashboards, filepath=filepath)

    def _valid_dashboards(self, dashboards: list):
        return isinstance(dashboards, list) and dashboards
