from dataclasses import dataclass


@dataclass(frozen=True, repr=True)
class GrafanaDashboard:
    uid: int
    title: str
    folder: str
    updater: str

    def __eq__(self, other):
        return self.uid == other.uid


@dataclass(frozen=True, repr=True)
class GrafanaPanel:
    id: int
    title: str

    def __eq__(self, other):
        return self.id == other.id


@dataclass(frozen=True, repr=True)
class GrafanaDataSource:
    id: int
    name: str
    type: str
    url: str

    def __eq__(self, other):
        return self.id == other.id
