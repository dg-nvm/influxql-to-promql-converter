def normalize_target_uid(target):
    if not target:
        return ""
    if "uid" in target.get("datasource", {}):
        uid = target["datasource"]["uid"]
    elif isinstance(target.get("datasource", False), str):
        uid = target["datasource"]
    else:
        return ""

    uid_normalized = uid.replace("$", "").replace("{", "").replace("}", "")
    return uid_normalized


class AdvancedInfluxDetection:

    def __init__(self, global_shared_state):
        self.global_shared_state = global_shared_state

    @property
    def datasources(self):
        return self.global_shared_state.get("grafana_datasources", {})

    def _ds_list_by_name(self):
        return {v["name"]: v for _, v in self.datasources.items()}

    def _basic_ds_type_check(self, target):
        return "dsType" in target and target["dsType"] == "influxdb"

    def _influxdb_datasource_type(self, target):
        return self._basic_ds_type_check(target) or (
            "datasource" in target
            and "type" in target["datasource"]
            and target["datasource"]["type"] == "influxdb"
        )

    def _uid_matches_influx_templating(self, target, templating):
        try:
            uid_normalized = normalize_target_uid(target)
            for item in templating["list"]:
                if (
                    item["name"] == uid_normalized
                    and item["type"] == "datasource"
                    and item["query"] == "influxdb"
                ):
                    return True
        except (IndexError, KeyError, TypeError):
            return False
        return False

    def is_target_influx(self, target, templating, panel_datasource):
        uid = normalize_target_uid(target)
        ds = target.get("datasource", {})
        is_ds_str = isinstance(ds, str)
        ret = (
            self._influxdb_datasource_type(target)
            or self._influxdb_like_by_uid(uid)
            or self._uid_matches_influx_templating(target, templating)
            or (is_ds_str and self._influxdb_like_by_ds_name(ds))
            or (self._influxdb_like_by_ds_name(uid))
        )
        if not ret and panel_datasource:
            pd = {"datasource": panel_datasource}
            pret = self.is_target_influx(pd, templating, None)
            if not ret and pret:
                return pret
        return ret

    def _influxdb_like_by_uid(self, uid, typ=None):
        ds_list = self.datasources
        if uid in ds_list and ds_list[uid]["type"] == "influxdb":
            if typ and typ != ds_list[uid]["type"]:
                self._error_manager.add_error(
                    "Panel Datasource Type mismatches the Grafana DS type - wrong panel, won't import",
                    error_level="WARN",
                )
                return False
            if ds_list[uid].get("jsonData", {}).get("version", "") == "Flux":
                return False
            return True
        return False

    def _influxdb_like_by_ds_name(self, ds):
        ds_list_by_name = self._ds_list_by_name()
        if ds in ds_list_by_name and ds_list_by_name[ds]["type"] == "influxdb":
            if ds_list_by_name[ds].get("jsonData", {}).get("version", "") == "Flux":
                return False
            return True
        else:
            return False
