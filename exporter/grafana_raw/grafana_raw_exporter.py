import json
import logging
from typing import Tuple, Union
import uuid
import requests

from ..exporter import Exporter


class GrafanaRawExporter(Exporter):
    def __init__(self, params, global_shared_state, log_level=logging.INFO):
        super().__init__(
            __name__,
            global_shared_state,
            log_level,
        )
        try:
            self._api_endpoint = params["endpoint"]
            self._auth_header_key = params["auth_header"]["key"]
            self._auth_header_value = params["auth_header"]["value"]
            if params.get("parent_folder_uid_from_shared_state", False):
                self._parent_folder_uid = lambda: self.global_shared_state[
                    "EXPORTERS_SHARED_STATE"
                ]["last_parent_uid"]
            elif "parent_folder_uid" in params:
                self._parent_folder_uid = lambda: params["parent_folder_uid"]
            else:
                self._parent_folder_uid = lambda: None
            self._api_headers = {
                self._auth_header_key: self._auth_header_value,
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "User-Agent": None,
            }
            self.organization_id = params.get("org_id", 1)
            self.folder_suffix = params["folder_suffix"]
        except KeyError as e:
            raise ValueError(str(e))

    def create_folder(self, folder_uid, folders):
        create_folder_url = self._api_endpoint + "/folders"

        folder_uid_string = str(folder_uid)
        request_json = {
            "title": f"{folders[folder_uid_string]}{self.folder_suffix}",
            "uid": folder_uid_string,
        }
        if self._parent_folder_uid():
            request_json["parentUid"] = self._parent_folder_uid()

        response = requests.post(
            create_folder_url,
            data=json.dumps(request_json),
            headers=self._api_headers,
            verify=False,
        )

        response.raise_for_status()

    def folder_by_name(
        self, name, parent_folder_uid=None
    ) -> Union[Tuple[int, str], Tuple[None, None]]:
        res = requests.get(
            f"{self._api_endpoint}/folders?orgId={self.organization_id}{'&parentUid=' + parent_folder_uid if parent_folder_uid else ''}",
            headers=self._api_headers,
            verify=False,
        )
        res.raise_for_status()

        folders = res.json()

        for f in folders:
            if f["title"] == name:
                return f["id"], f["uid"]
        return None, None

    # Creates dashboard in grafana
    def export_dashboards(self, dashboards, _folders):
        for dashboard in dashboards:
            folder_title = ""
            if "meta" in dashboard:
                folder_title = dashboard["meta"]["folderTitle"]
                dashboard = dashboard["dashboard"]
            dashboard["id"] = "null"
            dashboard["uid"] = str(uuid.uuid4())

            _id, uid = self.folder_by_name(
                f"{folder_title}{self.folder_suffix}", self._parent_folder_uid()
            )
            if uid is None and not f"{folder_title}{self.folder_suffix}" == "General":
                _id, uid = self.folder_by_name(
                    f"{folder_title}{self.folder_suffix}", self._parent_folder_uid()
                )
                if uid is None:
                    uid = str(uuid.uuid4())
                    self.create_folder(uid, {uid: folder_title})

            dashboard_json = {
                "dashboard": dashboard,
                "folderUid": str(uid) if uid is not None else self._parent_folder_uid(),
                "overwrite": True,
            }

            response = requests.post(
                self._api_endpoint + "/dashboards/db",
                data=json.dumps(dashboard_json),
                headers=self._api_headers,
                verify=False,
            )
            if response.status_code != 200:
                self._logger.error(
                    f"Error creating dashboard {dashboard['title']}: {response.content}"
                )
            else:
                self._logger.debug(
                    f"Successfully exported dashboard: {dashboard['title']}"
                )
