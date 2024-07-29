Grafana Importer
======================
This importer fetches dashboards from grafana API endpoint.

**Configuration Options**:
* ```endpoint``` (required): Grafana API URL.
* ```api_token``` (required): Authorization token for accessing grafana API.
* ```auth_header``` (optional): alternative to api_token, to login with username/password
* ```uid_filter_list``` (optional): list of dashboard uids to migrate
* ```auth_type``` (optional): override auth type, instead of default "Bearer ", can be used to remove auth_type and pass header fully
* ```use_switch_org_api``` (optional, default True): if set to false, will disable using switch-org api that is needed for multi-org Grafanas.
* ```cache_file``` (optional): path to file (relative to root of the migrator) where cache of imported objects will be kept. Usefull for debugging conversion without redownloading

**Example config**:
```
importer:
  grafana:
    endpoint: https://myusername.grafana.net
    api_token: <<grafana api token>>
```