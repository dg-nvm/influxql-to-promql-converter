import builtins
import json
import types
from typing import Tuple, Union
import uuid
import requests

from ..exporter import Exporter


class GrafanaFoldersExporter(Exporter):
    def __init__(self, params, global_shared_state, log_level):
        super().__init__(__name__, global_shared_state, log_level)
        try:
            self._api_endpoint = params["endpoint"]
            self._auth_header_key = params["auth_header"]["key"]
            self._auth_header_value = params["auth_header"]["value"]
            self._folder_structure = params["folder_structure"]
            self._last_folder_into_shared_state = params.get(
                "last_folder_into_shared_state", False
            )
            self._api_headers = {
                self._auth_header_key: self._auth_header_value,
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "User-Agent": None,
            }
            self._last_folder = None
        except KeyError as e:
            raise ValueError(str(e))

    def create_folder(self, folder_uid, folder_title, parent_uid=None):
        create_folder_url = self._api_endpoint + "/folders"

        folder_uid_string = str(folder_uid)
        request_json = {
            "title": folder_title,
            "uid": folder_uid_string,
            "parentUid": parent_uid,
        }

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

    def process_folder_structure(self, structure=None, prev_id=None):
        if structure is None:
            structure = self._folder_structure
        elif structure is {}:
            return
        for folder_title, children in structure.items():
            _id, uid = self.folder_by_name(folder_title, prev_id)
            if uid is None and not folder_title == "General":
                _id, uid = self.folder_by_name(folder_title, prev_id)
                if uid is None:
                    uid = str(uuid.uuid4())
                    self.create_folder(uid, folder_title, prev_id)
                    self._logger.info(f"Created folder {folder_title} - {uid}")
            else:
                self._logger.info(
                    f"Skipping creation of {folder_title} - {uid} - already exists"
                )

            self._last_folder = uid

            match type(children):
                case builtins.str:
                    children = {children: {}}
                case builtins.dict:
                    # valid case
                    pass
                case types.NoneType:
                    continue
                case _:
                    raise ValueError(
                        f"Invalid structure object {str(children)} of type {type(children)}"
                    )
            self.process_folder_structure(structure=children, prev_id=uid)

    # Creates dashboard in grafana
    def export_dashboards(self, _dashboards, folders):
        self._folders = folders
        self.process_folder_structure()
        if self._last_folder_into_shared_state:
            self.global_shared_state["EXPORTERS_SHARED_STATE"][
                "last_parent_uid"
            ] = self._last_folder
