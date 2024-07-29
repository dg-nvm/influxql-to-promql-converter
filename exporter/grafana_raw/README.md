Grafana Raw Exporter
======================
This exporter export dashboards to grafana API endpoint without doing Migration Folder first,
it does export dashboards as-they-are including folder

**Configuration Options**:
* ```endpoint``` (required): Grafana API URL.
* ```auth_header.key``` (required): Authorization header key for accessing grafana API.
* ```auth_header.value``` (required): Authorization header value for accessing grafana API - must contain token.
* ```parent_folder_uid``` (optional): uid of folder to put dashboards in
* ```parent_folder_uid_from_shared_state``` (optional): if set it will use last folder created by Grafana Folders exporter (when set to write to shared state)
* ```org_id``` (optional): org_id to write to
* ```folder_suffix``` (optional): will add suffix into folder like "_MIGRATED"

**Example config**:
```
exporter:
  grafana:
    endpoint: https://myusername.grafana.net
    auth_header:
      key: Authorization
      value: Bearer <<grafana api token>>
```

**Example config for Logzio API**:
```
exporter:
  grafana:
    endpoint: https://api.logz.io/v1/grafana/api # https://api-<<region>>.logz.io/v1/grafana/api for non us regions
    auth_header:
      key: X-API-TOKEN
      value: <<logzio grafana api token>>
```
