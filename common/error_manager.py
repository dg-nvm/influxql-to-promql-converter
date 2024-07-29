from dataclasses import dataclass
from contextvars import ContextVar
from pathlib import Path
import sys
from typing import Optional

file = Path(__file__).resolve()
parent, root = file.parent, file.parents[2]
sys.path.append(str(root))

try:
    sys.path.remove(str(parent))
except ValueError:
    pass

from .grafana_model import GrafanaDashboard, GrafanaPanel


@dataclass(frozen=True, slots=True)
class ConversionError:
    message: str
    dashboard: GrafanaDashboard
    panel: GrafanaPanel
    link: str
    notes: str = ""
    error_level: str = "INFO"

    def csv(self):
        return f"{self.c(self.message)},{self.c(self.dashboard.folder)},{self.c(self.dashboard.title)},{self.c(self.panel.title)},{self.c(self.error_level)},{self.c(self.notes)},{self.c(self.dashboard.updater)},{self.c(self.link)}"

    def c(self, s):
        return str(s).replace(",", "")

    @classmethod
    def csv_header(cls):
        return "Message,Folder,Dashboard,Panel,ErrorLevel,Notes,Updater,Link"


class ProcessingContext:
    def __init__(
        self,
        dashboard: Optional[GrafanaDashboard] = None,
        meta: Optional[dict] = None,
        panel: Optional[GrafanaPanel] = None,
        folder: Optional[str] = None,
        team: Optional[str] = None,
        grafana_url: Optional[str] = None,
        grafana_organization_id: Optional[str | int] = None,
    ):
        self.dashboard = dashboard
        self.meta = meta
        self.panel = panel
        self.folder = folder
        self.team = team
        self.grafana_url = grafana_url
        self.grafana_organization_id = grafana_organization_id

    def __str__(self):
        return f"ProcessingContext[Dashboard:{self.dashboard.title if self.dashboard else 'None'},Panel:{self.panel.title if self.panel else 'None'},Folder:{self.folder},DashUID:{self.dashboard.uid if self.dashboard else 'None'}]"


class ErrorManager:
    def __init__(self, logger, processing_context: ProcessingContext):
        self._conversion_errors = []
        self._logger = logger
        self.context = processing_context

    def errors_csv(self) -> str:
        if self._conversion_errors:
            return "\n".join(
                [ConversionError.csv_header()]
                + [x.csv() for x in self._conversion_errors]
            )
        else:
            return ""

    def _debug_link(self):
        if (
            self.context.dashboard
            and self.context.panel
            and self.context.grafana_organization_id
            and self.context.grafana_url
        ):
            return f"{self.context.grafana_url}/d/{self.context.dashboard.uid}?panelId={self.context.panel.id}&editPanel={self.context.panel.id}&fullscreen&edit&tab=alert&orgId={self.context.grafana_organization_id}"
        else:
            return "Cannot-Calculate-Link"

    def add_error(self, msg, error_level="INFO", notes=""):
        self._logger.error(msg)
        msg = msg.replace(",", "-")
        if self.context.dashboard and self.context.panel:
            self._conversion_errors.append(
                ConversionError(
                    error_level=error_level,
                    message=msg,
                    notes=notes,
                    dashboard=self.context.dashboard,
                    panel=self.context.panel,
                    link=self._debug_link(),
                )
            )
