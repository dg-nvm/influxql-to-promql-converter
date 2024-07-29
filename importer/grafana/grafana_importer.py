import logging

from ..importer import Importer
from ..cache import DashboardsCache, GeneralCache
import json
import requests


class GrafanaImporter(Importer):
    API_HEADERS = {
        "Authorization": "",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    def __init__(self, params: dict, global_shared_state: dict, log_level=logging.INFO):
        super().__init__(__name__, global_shared_state, log_level)
        try:
            self._grafana_endpoint = params["endpoint"]
            self._grafana_api_token = params["api_token"]
            self._grafana_auth_header = params.get("auth_header", None)
            self._organization_id = params["orgId"]
            self.uid_filter_list = params.get("uid_filter_list", set())

            if self._grafana_auth_header:
                self.API_HEADERS["Authorization"] = self._grafana_auth_header
            else:
                self.API_HEADERS["Authorization"] = (
                    params.get("auth_type", "Bearer ") + self._grafana_api_token
                )

            self._use_switch_org_api = params.get("use_switch_org_api", True)

            if cache_file := params.get("cache_file", None):
                self._cache = DashboardsCache(cache_file)
                self._folders_cache = GeneralCache(
                    lambda x: isinstance(x, dict) and x, f"{cache_file}.folders"
                )
                self._datasources_cache = GeneralCache(
                    lambda x: isinstance(x, dict) and x, f"{cache_file}.datasources"
                )
            else:
                self._cache = None
                self._folders_cache = None
                self._datasources_cache = None

            self.requests = requests.Session()
        except KeyError as e:
            raise ValueError(str(e))

    def fetch_dashboards(self):
        raise NotImplementedError

    def should_be_filtered_out(self, uid):
        return self.uid_filter_list and uid not in self.uid_filter_list

    def fetch_dashboards_and_folders(self, no_cache: bool = False):
        if not no_cache and self._cache and self._cache.cache_available():
            dashboards = self._cache.load()
        else:
            dashboards = self._build_dashboards_list()
            self._cache and self._cache.save(dashboards)

        c = []
        for d in dashboards:
            if self.should_be_filtered_out(d["dashboard"]["uid"]):
                continue
            c.append(d)

        if (
            not no_cache
            and self._folders_cache
            and self._folders_cache.cache_available()
        ):
            folders = self._folders_cache.load()
        else:
            folders = self._build_folder_list()
            self._folders_cache and self._folders_cache.save(folders)

        if (
            not no_cache
            and self._datasources_cache
            and self._datasources_cache.cache_available()
        ):
            datasources = self._datasources_cache.load()
        else:
            datasources = self._get_datasources_list()
            self._datasources_cache and self._datasources_cache.save(datasources)

        if datasources:
            self.global_shared_state["grafana_datasources"] = datasources

        return c, folders

    def _get_datasources_list(self):
        self._switch_org()

        response = self.requests.get(
            f"{self._grafana_endpoint}/api/datasources?limit=5000&orgId={self._organization_id}",
            headers=self.API_HEADERS,
            verify=False,
        )

        response.raise_for_status()

        datasources = {}
        for ds in response.json():
            datasources[ds["uid"]] = {
                "id": ds.get("id"),
                "uid": ds.get("uid"),
                "name": ds.get("name"),
                "type": ds.get("type"),
                "typeName": ds.get("typeName"),
                "jsonData": ds.get("jsonData"),
            }

        return datasources

    def _extract_dashboard_uids(self, response):
        response_json = json.loads(response.content)
        uid_list = []
        for dashboard in response_json:
            if dashboard["type"] == "dash-db":
                uid_list.append(dashboard.get("uid"))
        return uid_list

    def _switch_org(self):
        if not self._use_switch_org_api:
            return
        r = self.requests.post(
            f"{self._grafana_endpoint}/api/user/using/{self._organization_id}",
            headers=self.API_HEADERS,
        )
        r.raise_for_status()

        r = self.requests.get(
            f"{self._grafana_endpoint}/api/org",
            headers=self.API_HEADERS,
        )
        r.raise_for_status()

        current_org = r.json()["id"]
        if int(current_org) != int(self._organization_id):
            raise RuntimeError(f"Cannot switch organization to {self._organization_id}")

    # Builds list of dashboards from logzio grafana grafana
    def _build_dashboards_list(self) -> list:
        self._switch_org()

        response = self.requests.get(
            f"{self._grafana_endpoint}/api/search?limit=5000&orgId={self._organization_id}",
            headers=self.API_HEADERS,
            verify=False,
        )

        dashboards_uids = self._extract_dashboard_uids(response)
        dashboards = []
        base_url = f'{self._grafana_endpoint}/api/dashboards/uid/{{uid}}?orgId={self._organization_id}"'
        for i, uid in enumerate(dashboards_uids):
            if self.should_be_filtered_out(uid):
                continue
            response = self.requests.get(
                base_url.format(uid=uid),
                headers=self.API_HEADERS,
                verify=False,
            )
            dashboard = response.json()
            # Skip already migrated to allow conversion inside same grafana - DG
            if "meta" not in dashboard:
                self._logger.critical(f"No meta in dashboard {uid}")
                continue
            dashboards.append(dashboard)
        return dashboards

    def _build_folder_list(self) -> dict:
        self._switch_org()

        res = self.requests.get(
            f"{self._grafana_endpoint}/api/search?type=folder-db&orgId={self._organization_id}",
            headers=self.API_HEADERS,
            verify=False,
        )
        res.raise_for_status()

        folders = res.json()

        return {folder["uid"]: folder["title"] for folder in folders}
